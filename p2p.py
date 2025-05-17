import socket
import threading
import sys
import os
import time
import json
import hashlib
import math
import random
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kademlia.network import Server as DHTServer
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Configurações globais
MAX_WORKERS = 20  # Limite de threads no pool
MAX_ARQUIVO_MEMORIA = 100 * 1024 * 1024  # 100 MB em memória
CHUNK_SIZE = 8192  # 8KB por chunk
NOME_PASTA = "shared_folder"
PASTA = os.path.normpath(os.path.join(os.getcwd(), NOME_PASTA))

# Variáveis globais
conexoes_ativas = []
arquivos_info = {}  # nome_arquivo -> ArquivoInfo
arquivos_recentes = {}  # hash -> timestamp
dht_loop = None
dht_node = None
thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)

class ArquivoInfo:
    def __init__(self, nome, timestamp, hash_valor, tamanho=0):
        self.nome = nome
        self.timestamp = timestamp
        self.hash = hash_valor
        self.tamanho = tamanho

def normalizar_caminho(caminho):
    """
    Normaliza um caminho para o formato do sistema operacional atual
    """
    return os.path.normpath(caminho)

def calcular_hash_arquivo(caminho):
    """
    Calcula o hash MD5 de um arquivo lendo-o em chunks
    """
    md5 = hashlib.md5()
    with open(caminho, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            md5.update(chunk)
    return md5.hexdigest()

def calcular_hash(dados):
    """
    Calcula o hash MD5 de dados em memória
    """
    return hashlib.md5(dados).hexdigest()



def enviar_arquivo(conn, nome_arquivo, caminho):
    """
    Envia um arquivo em chunks com verificação de integridade
    """
    try:
        tamanho = os.path.getsize(caminho)
        hash_arquivo = calcular_hash_arquivo(caminho)
        timestamp = time.time()
        
        # Envia cabeçalho com nome, tamanho, hash e timestamp
        header = f"[ARQUIVO]{nome_arquivo}|{tamanho}|{hash_arquivo}|{timestamp}|".encode()
        conn.sendall(header)
        
        # Envia conteúdo em chunks
        bytes_enviados = 0
        with open(caminho, "rb") as f:
            while bytes_enviados < tamanho:
                chunk = f.read(CHUNK_SIZE)
                if not chunk:
                    break
                conn.sendall(chunk)
                bytes_enviados += len(chunk)
        
        print(f"[ENVIADO] '{nome_arquivo}' ({tamanho} bytes) enviado com sucesso")
        
        # Atualiza informações locais
        arquivos_info[nome_arquivo] = ArquivoInfo(nome_arquivo, timestamp, hash_arquivo, tamanho)
        
        # Publica na DHT
        executar_na_dht(publicar_arquivo_na_dht(nome_arquivo, timestamp, hash_arquivo))
        
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao enviar '{nome_arquivo}': {e}")
        return False

def propagar_arquivo(nome_arquivo, caminho, origem=None):
    """
    Propaga o arquivo para um subconjunto de peers usando protocolo gossip
    """
    # Seleciona um subconjunto aleatório de peers (log N)
    num_peers = len(conexoes_ativas)
    if num_peers <= 1:
        return
        
    # Logaritmo na base 2 do número de peers, no mínimo 3
    num_targets = max(3, int(math.log2(num_peers)))
    targets = random.sample(conexoes_ativas, min(num_targets, num_peers))
    
    for conn in targets:
        try:
            if conn != origem:  # Não envia de volta para quem enviou
                enviar_arquivo(conn, nome_arquivo, caminho)
        except Exception as e:
            print(f"[ERRO] Falha ao propagar para um peer: {e}")

class FileChangeHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        caminho = normalizar_caminho(event.src_path)
        nome_arquivo = os.path.basename(caminho)

        # Espera o arquivo estabilizar
        tamanho_anterior = -1
        for _ in range(10):
            try:
                tamanho_atual = os.path.getsize(caminho)
                if tamanho_atual == tamanho_anterior:
                    break 
                tamanho_anterior = tamanho_atual
            except FileNotFoundError:
                pass 
            time.sleep(0.5)
        else:
            print(f"[ERRO] Arquivo '{nome_arquivo}' não estabilizou para leitura.")
            return

        # Verifica se o arquivo existe e não está vazio
        try:
            tamanho = os.path.getsize(caminho)
            if tamanho == 0:
                print(f"[AVISO] Arquivo '{nome_arquivo}' está vazio. Ignorando envio.")
                return
        except Exception as e:
            print(f"[ERRO] Falha ao verificar '{nome_arquivo}': {e}")
            return

        print(f"[NOVO ARQUIVO] Propagando '{nome_arquivo}' aos peers...")
        
        # Propaga o arquivo usando protocolo gossip
        propagar_arquivo(nome_arquivo, caminho)

def iniciar_monitoramento(pasta):
    event_handler = FileChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path=pasta, recursive=False)
    observer.start()
    print(f"[MONITORANDO] Pasta: {pasta}")
    return observer

def handle_client(client_socket):
    buffer = b""
    arquivo_atual = None
    tamanho_esperado = 0
    hash_esperado = None
    timestamp_esperado = 0
    bytes_recebidos = 0
    
    while True:
        try:
            data = client_socket.recv(CHUNK_SIZE)
            if not data:
                break
                
            buffer += data
            
            # Processa comandos especiais
            if buffer.startswith(b"[LISTAR_ARQUIVOS]") and len(buffer) == 17:
                # Envia lista de arquivos
                arquivos = listar_arquivos_locais()
                resposta = f"[LISTA_ARQUIVOS]{json.dumps(arquivos)}".encode()
                client_socket.sendall(resposta)
                buffer = b""
                continue
                
            if buffer.startswith(b"[SOLICITAR_ARQUIVO]"):
                # Extrai nome do arquivo solicitado
                nome_arquivo = buffer[19:].decode()
                buffer = b""
                
                caminho = os.path.join(PASTA, nome_arquivo)
                if os.path.exists(caminho) and os.path.isfile(caminho):
                    print(f"[SOLICITAÇÃO] Enviando arquivo '{nome_arquivo}'")
                    enviar_arquivo(client_socket, nome_arquivo, caminho)
                else:
                    client_socket.sendall(f"[ERRO]Arquivo '{nome_arquivo}' não encontrado".encode())
                continue
                
            if buffer.startswith(b"[LISTA_ARQUIVOS]"):
                # Processa lista de arquivos recebida
                try:
                    # Encontra o final do JSON
                    json_start = 16  # Comprimento de "[LISTA_ARQUIVOS]"
                    json_data = buffer[json_start:].decode()
                    arquivos_remotos = json.loads(json_data)
                    
                    print(f"\n[RECEBIDO] Lista de {len(arquivos_remotos)} arquivos disponíveis:")
                    for arq in arquivos_remotos:
                        print(f"  - {arq['nome']} ({arq['tamanho']} bytes, modificado em {time.ctime(arq['timestamp'])})")
                    
                    buffer = b""
                except Exception as e:
                    # Se não conseguir decodificar o JSON, pode ser que ainda não recebeu tudo
                    # Continua recebendo dados
                    pass
                continue
            
            # Se ainda não estamos processando um arquivo, procura por cabeçalho
            if arquivo_atual is None and b"|" in buffer:
                if buffer.startswith(b"[ARQUIVO]"):
                    try:
                        # Encontra o final do cabeçalho (último |)
                        header_end = buffer.find(b"|", buffer.rfind(b"|", 0, 100) + 1) + 1
                        header = buffer[:header_end].decode()
                        partes = header.replace("[ARQUIVO]", "").split("|")
                        
                        if len(partes) >= 4:  # Nome, tamanho, hash, timestamp
                            arquivo_atual = partes[0]
                            tamanho_esperado = int(partes[1])
                            hash_esperado = partes[2]
                            timestamp_esperado = float(partes[3])
                            
                            # Verifica se já temos este arquivo e se o recebido é mais recente (last write wins)
                            if arquivo_atual in arquivos_info:
                                if timestamp_esperado <= arquivos_info[arquivo_atual].timestamp:
                                    print(f"[IGNORADO] Já temos uma versão mais recente de '{arquivo_atual}'")
                                    # Ignora este arquivo
                                    arquivo_atual = None
                                    buffer = buffer[header_end:]
                                    continue
                            
                            # Verifica se já recebemos este arquivo recentemente (últimos 5 minutos)
                            agora = time.time()
                            if hash_esperado in arquivos_recentes:
                                if agora - arquivos_recentes[hash_esperado] < 300:  # 5 minutos
                                    print(f"[DUPLICADO] Arquivo com hash {hash_esperado} já recebido recentemente")
                                    # Ignora este arquivo
                                    arquivo_atual = None
                                    buffer = buffer[header_end:]
                                    continue
                            
                            # Remove cabeçalho do buffer
                            buffer = buffer[header_end:]
                            bytes_recebidos = len(buffer)
                            
                            print(f"[RECEBENDO] Arquivo '{arquivo_atual}' ({tamanho_esperado} bytes)")
                    except Exception as e:
                        print(f"[ERRO] Falha ao processar cabeçalho: {e}")
                        buffer = buffer[10:]  # Avança para tentar encontrar outro cabeçalho
                        continue
            
            # Se estamos processando um arquivo, conta bytes recebidos
            elif arquivo_atual is not None:
                bytes_recebidos = len(buffer)
                
                # Se recebemos o arquivo completo
                if bytes_recebidos >= tamanho_esperado:
                    conteudo_arquivo = buffer[:tamanho_esperado]
                    buffer = buffer[tamanho_esperado:]
                    
                    # Verifica integridade
                    hash_calculado = calcular_hash(conteudo_arquivo)
                    if hash_calculado == hash_esperado:
                        # Salva arquivo
                        caminho_arquivo = os.path.join(PASTA, arquivo_atual)
                        with open(caminho_arquivo, "wb") as f:
                            f.write(conteudo_arquivo)
                        print(f"[RECEBIDO] Arquivo '{arquivo_atual}' verificado e salvo.")
                        
                        # Atualiza informações locais
                        arquivos_info[arquivo_atual] = ArquivoInfo(
                            arquivo_atual, timestamp_esperado, hash_esperado, tamanho_esperado
                        )
                        
                        # Registra o hash como recentemente recebido
                        arquivos_recentes[hash_esperado] = time.time()
                        
                        # Publica na DHT
                        executar_na_dht(publicar_arquivo_na_dht(
                            arquivo_atual, timestamp_esperado, hash_esperado
                        ))
                        
                        # Propaga para outros peers (protocolo gossip)
                        propagar_arquivo(arquivo_atual, caminho_arquivo, origem=client_socket)
                    else:
                        print(f"[ERRO] Verificação falhou para '{arquivo_atual}'")
                        print(f"Hash esperado: {hash_esperado}")
                        print(f"Hash calculado: {hash_calculado}")
                    
                    # Limpa hashes antigos periodicamente
                    agora = time.time()
                    for hash_valor in list(arquivos_recentes.keys()):
                        if agora - arquivos_recentes[hash_valor] > 3600:  # 1 hora
                            del arquivos_recentes[hash_valor]
                    
                    # Reseta variáveis para próximo arquivo
                    arquivo_atual = None
                    tamanho_esperado = 0
                    hash_esperado = None
                    timestamp_esperado = 0
                    bytes_recebidos = 0
                    
        except Exception as e:
            print(f"[ERRO] Falha no cliente: {e}")
            break
    
    # Remove da lista de conexões ativas
    try:
        if client_socket in conexoes_ativas:
            conexoes_ativas.remove(client_socket)
    except:
        pass
    
    try:
        client_socket.close()
    except:
        pass

def start_server(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(('0.0.0.0', port))
    server.listen(5)
    print(f"[SERVIDOR] Escutando na porta {port}...")

    while True:
        try:
            client, addr = server.accept()
            print(f"[NOVA CONEXÃO] {addr}")
                
            conexoes_ativas.append(client)
            
            # Usa o pool de threads em vez de criar uma nova thread para cada conexão
            thread_pool.submit(handle_client, client)
        except Exception as e:
            print(f"[ERRO] Falha ao aceitar conexão: {e}")

def connect_to_peer(ip, port):
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((ip, port))
        conexoes_ativas.append(client)
        print(f"[CONECTADO] ao peer {ip}:{port}")
        
        # Usa o pool de threads
        thread_pool.submit(handle_client, client)
        
        return client
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar: {e}")
        return None

def listar_arquivos_locais():
    """
    Retorna uma lista de arquivos na pasta compartilhada
    """
    arquivos = []
    for nome_arquivo in os.listdir(PASTA):
        caminho = os.path.join(PASTA, nome_arquivo)
        if os.path.isfile(caminho):
            tamanho = os.path.getsize(caminho)
            timestamp = os.path.getmtime(caminho)
            hash_valor = calcular_hash_arquivo(caminho)
            arquivos.append({
                "nome": nome_arquivo,
                "tamanho": tamanho,
                "timestamp": timestamp,
                "hash": hash_valor
            })
    return arquivos

def solicitar_lista_arquivos(conn):
    """
    Solicita lista de arquivos de um peer
    """
    try:
        conn.sendall(b"[LISTAR_ARQUIVOS]")
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao solicitar lista de arquivos: {e}")
        return False

def solicitar_arquivo(conn, nome_arquivo):
    """
    Solicita um arquivo específico de um peer
    """
    try:
        mensagem = f"[SOLICITAR_ARQUIVO]{nome_arquivo}".encode()
        conn.sendall(mensagem)
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao solicitar arquivo: {e}")
        return False

# Funções para DHT
async def publicar_arquivo_na_dht(nome_arquivo, timestamp, hash_valor):
    """
    Publica informações do arquivo na DHT
    """
    # Formato: "nome_arquivo|timestamp|hash"
    valor = f"{nome_arquivo}|{timestamp}|{hash_valor}"
    await dht_node.set(nome_arquivo, valor)
    print(f"[DHT] Publicado '{nome_arquivo}' na DHT")

async def verificar_arquivo_na_dht(nome_arquivo):
    """
    Verifica se existe uma versão mais recente do arquivo na DHT
    """
    resultado = await dht_node.get(nome_arquivo)
    if not resultado:
        return None
        
    partes = resultado.split("|")
    if len(partes) >= 3:
        return {
            "nome": partes[0],
            "timestamp": float(partes[1]),
            "hash": partes[2]
        }
    return None

def iniciar_dht_loop():
    """
    Inicia o loop de eventos asyncio para DHT em uma thread separada
    """
    global dht_loop, dht_node
    
    dht_loop = asyncio.new_event_loop()
    asyncio.set_event_loop(dht_loop)
    
    dht_node = DHTServer()
    dht_loop.run_until_complete(dht_node.listen(PORTA_LOCAL + 1000))
    
    # Se tiver bootstrap node
    if len(sys.argv) >= 4:
        bootstrap_ip = sys.argv[2]
        bootstrap_port = int(sys.argv[3])
        dht_loop.run_until_complete(dht_node.bootstrap([(bootstrap_ip, bootstrap_port + 1000)]))
    
    # Mantém o loop rodando
    dht_loop.run_forever()

def executar_na_dht(coroutine):
    """
    Executa uma coroutine no loop DHT e retorna o resultado
    """
    try:
        future = asyncio.run_coroutine_threadsafe(coroutine, dht_loop)
        return future.result(timeout=10)  # timeout de 10 segundos
    except Exception as e:
        print(f"[ERRO DHT] {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python p2p.py <porta>")
        print("Porta Recomendada: 5000")
        sys.exit(1)

    PORTA_LOCAL = int(sys.argv[1])

    if not os.path.exists(PASTA):
        os.makedirs(PASTA)
        print(f"[CRIADO] Pasta '{NOME_PASTA}' criada em: {PASTA}")
    else:
        print(f"[EXISTE] Pasta encontrada: {PASTA}")

    # Inicia o loop DHT em uma thread separada
    dht_thread = threading.Thread(target=iniciar_dht_loop, daemon=True)
    dht_thread.start()
    time.sleep(1)  # Espera o DHT inicializar


    # Inicia servidor em uma thread separada
    threading.Thread(target=start_server, args=(PORTA_LOCAL,), daemon=True).start()
    
    # Inicia monitoramento da pasta
    observer = iniciar_monitoramento(PASTA)

    try:
        while True:
            print("\n--- MENU ---")
            print("1. Conectar a outro peer")
            print("2. Procurar arquivo na DHT")
            print("3. Listar arquivos disponíveis em um peer")
            print("4. Solicitar arquivo específico de um peer")
            print("5. Listar arquivos locais")
            print("6. Sair")
            escolha = input("Escolha uma opção: ")

            if escolha == "1":
                ip = input("IP do peer: ")
                porta = int(input("Porta do peer: "))
                peer = connect_to_peer(ip, porta)
                if peer:
                    # Bootstrapping do DHT
                    executar_na_dht(dht_node.bootstrap([(ip, porta + 1000)]))
            
            elif escolha == "2":
                nome_arquivo = input("Nome do arquivo para buscar: ")
                resultado = executar_na_dht(verificar_arquivo_na_dht(nome_arquivo))
                if resultado:
                    print(f"[ENCONTRADO] Arquivo '{nome_arquivo}':")
                    print(f"  Timestamp: {time.ctime(resultado['timestamp'])}")
                    print(f"  Hash: {resultado['hash']}")
                else:
                    print("[NÃO ENCONTRADO] Nenhum peer possui esse arquivo na DHT.")
            
            elif escolha == "3":
                if not conexoes_ativas:
                    print("Não há peers conectados.")
                else:
                    print("Peers conectados:")
                    for i, conn in enumerate(conexoes_ativas):
                        try:
                            peer_addr = conn.getpeername()
                            print(f"{i+1}. {peer_addr[0]}:{peer_addr[1]}")
                        except:
                            print(f"{i+1}. (Conexão inválida)")
                    
                    try:
                        idx = int(input("Escolha um peer (número): ")) - 1
                        if 0 <= idx < len(conexoes_ativas):
                            solicitar_lista_arquivos(conexoes_ativas[idx])
                        else:
                            print("Índice inválido.")
                    except ValueError:
                        print("Entrada inválida.")
            
            elif escolha == "4":
                if not conexoes_ativas:
                    print("Não há peers conectados.")
                else:
                    print("Peers conectados:")
                    for i, conn in enumerate(conexoes_ativas):
                        try:
                            peer_addr = conn.getpeername()
                            print(f"{i+1}. {peer_addr[0]}:{peer_addr[1]}")
                        except:
                            print(f"{i+1}. (Conexão inválida)")
                    
                    try:
                        idx = int(input("Escolha um peer (número): ")) - 1
                        if 0 <= idx < len(conexoes_ativas):
                            nome_arquivo = input("Nome do arquivo a solicitar: ")
                            solicitar_arquivo(conexoes_ativas[idx], nome_arquivo)
                        else:
                            print("Índice inválido.")
                    except ValueError:
                        print("Entrada inválida.")
            
            elif escolha == "5":
                arquivos = listar_arquivos_locais()
                print(f"\nArquivos locais ({len(arquivos)}):")
                for arq in arquivos:
                    print(f"  - {arq['nome']} ({arq['tamanho']} bytes, modificado em {time.ctime(arq['timestamp'])})")
            
            elif escolha == "6":
                print("Encerrando...")
                break
            
            else:
                print("Opção inválida.")
    
    except KeyboardInterrupt:
        print("\nEncerrando...")
    
    finally:
        # Limpa recursos
        observer.stop()
        observer.join()
        
        for conn in conexoes_ativas:
            try:
                conn.close()
            except:
                pass
        
        thread_pool.shutdown(wait=False)
