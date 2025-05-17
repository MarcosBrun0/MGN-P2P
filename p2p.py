import socket
import threading
import sys
import os
import time
import json
import hashlib
import math
import random
import struct
import queue
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from kademlia.network import Server as DHTServer
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Configurações globais
MAX_CONEXOES = 100  # Limite de peers conforme requisito
MAX_WORKERS = 20  # Limite de threads no pool
MAX_ARQUIVO_MEMORIA = 100 * 1024 * 1024  # 100 MB em memória
CHUNK_SIZE = 8192  # 8KB por chunk
NOME_PASTA = "shared_folder"
PASTA = os.path.normpath(os.path.join(os.getcwd(), NOME_PASTA))
DISCOVERY_SERVER = "stun4.l.google.com"  # Servidor STUN público
DISCOVERY_PORT = 19302
RETRY_INTERVAL = 5  # Segundos entre tentativas de hole punching
MAX_PACKET_SIZE = 65507  # Tamanho máximo de pacote UDP

# Variáveis globais
peers = {}  # addr -> PeerInfo
arquivos_info = {}  # nome_arquivo -> ArquivoInfo
arquivos_recentes = {}  # hash -> timestamp
dht_loop = None
dht_node = None
thread_pool = ThreadPoolExecutor(max_workers=MAX_WORKERS)
udp_socket = None
local_ext_addr = None  # Endereço externo (público) local
transfer_queues = {}  # addr -> Queue
transfer_threads = {}  # addr -> Thread
running = True

class PeerInfo:
    def __init__(self, addr, ext_addr=None, last_seen=None):
        self.addr = addr  # Endereço local (ip, porta)
        self.ext_addr = ext_addr  # Endereço externo (ip, porta)
        self.last_seen = last_seen or time.time()
        self.active = True

class ArquivoInfo:
    def __init__(self, nome, timestamp, hash_valor, tamanho=0):
        self.nome = nome
        self.timestamp = timestamp
        self.hash = hash_valor
        self.tamanho = tamanho

class TransferInfo:
    def __init__(self, nome_arquivo, tamanho, hash_valor, timestamp, total_chunks):
        self.nome_arquivo = nome_arquivo
        self.tamanho = tamanho
        self.hash = hash_valor
        self.timestamp = timestamp
        self.total_chunks = total_chunks
        self.chunks_recebidos = set()
        self.buffer = bytearray(tamanho)
        self.completo = False

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

def get_external_ip():
    """
    Obtém o endereço IP externo e porta usando um servidor STUN
    """
    global local_ext_addr
    
    try:
        # Cria um socket UDP
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(5)
        
        # Envia uma solicitação para o servidor STUN
        sock.sendto(b'', (DISCOVERY_SERVER, DISCOVERY_PORT))
        
        # Recebe a resposta que contém nosso endereço externo
        data, addr = sock.recvfrom(1024)
        
        # Extrai o endereço IP e porta da resposta
        # Nota: Esta é uma implementação simplificada; um cliente STUN real
        # analisaria o formato de mensagem STUN completo
        local_ext_addr = addr
        
        print(f"[STUN] Endereço externo detectado: {local_ext_addr}")
        return local_ext_addr
    except Exception as e:
        print(f"[ERRO STUN] {e}")
        return None
    finally:
        sock.close()

def iniciar_udp_socket(porta):
    """
    Inicia o socket UDP para comunicação
    """
    global udp_socket
    
    try:
        udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp_socket.bind(('0.0.0.0', porta))
        print(f"[UDP] Socket iniciado na porta {porta}")
        return udp_socket
    except Exception as e:
        print(f"[ERRO UDP] {e}")
        sys.exit(1)

def enviar_pacote(addr, tipo, dados):
    """
    Envia um pacote UDP formatado
    """
    try:
        # Formato: [tipo:2][dados]
        header = struct.pack('!H', tipo)
        pacote = header + dados
        udp_socket.sendto(pacote, addr)
        return True
    except Exception as e:
        print(f"[ERRO ENVIO] {e}")
        return False

def enviar_heartbeat(addr):
    """
    Envia um heartbeat para manter a conexão ativa
    """
    enviar_pacote(addr, 1, b'')  # Tipo 1: Heartbeat

def enviar_discovery(addr):
    """
    Envia uma solicitação de descoberta
    """
    # Inclui nosso endereço externo na solicitação
    if local_ext_addr:
        dados = f"{local_ext_addr[0]}:{local_ext_addr[1]}".encode()
        enviar_pacote(addr, 2, dados)  # Tipo 2: Discovery

def enviar_discovery_response(addr):
    """
    Envia uma resposta de descoberta
    """
    if local_ext_addr:
        dados = f"{local_ext_addr[0]}:{local_ext_addr[1]}".encode()
        enviar_pacote(addr, 3, dados)  # Tipo 3: Discovery Response

def iniciar_hole_punching(ext_addr):
    """
    Inicia o processo de hole punching para um endereço externo
    """
    try:
        ip, porta = ext_addr
        print(f"[HOLE PUNCHING] Iniciando para {ext_addr}")
        
        # Envia pacotes para abrir o NAT
        for _ in range(5):
            enviar_pacote(ext_addr, 4, b'')  # Tipo 4: Hole Punching
            time.sleep(0.2)
        
        return True
    except Exception as e:
        print(f"[ERRO HOLE PUNCHING] {e}")
        return False

def processar_pacote(dados, addr):
    """
    Processa um pacote UDP recebido
    """
    try:
        if len(dados) < 2:
            return
        
        tipo = struct.unpack('!H', dados[:2])[0]
        conteudo = dados[2:]
        
        # Atualiza ou adiciona o peer
        if addr not in peers:
            peers[addr] = PeerInfo(addr)
            print(f"[NOVO PEER] {addr}")
        else:
            peers[addr].last_seen = time.time()
        
        # Processa com base no tipo
        if tipo == 1:  # Heartbeat
            # Apenas atualiza o timestamp do peer
            pass
            
        elif tipo == 2:  # Discovery Request
            # Extrai o endereço externo do peer
            try:
                ext_addr_str = conteudo.decode()
                ip, porta = ext_addr_str.split(':')
                peers[addr].ext_addr = (ip, int(porta))
                print(f"[DISCOVERY] Peer {addr} tem endereço externo {peers[addr].ext_addr}")
                
                # Envia resposta de discovery
                enviar_discovery_response(addr)
                
                # Inicia hole punching se necessário
                if peers[addr].ext_addr:
                    iniciar_hole_punching(peers[addr].ext_addr)
            except:
                pass
                
        elif tipo == 3:  # Discovery Response
            # Extrai o endereço externo do peer
            try:
                ext_addr_str = conteudo.decode()
                ip, porta = ext_addr_str.split(':')
                peers[addr].ext_addr = (ip, int(porta))
                print(f"[DISCOVERY] Peer {addr} tem endereço externo {peers[addr].ext_addr}")
                
                # Inicia hole punching
                if peers[addr].ext_addr:
                    iniciar_hole_punching(peers[addr].ext_addr)
            except:
                pass
                
        elif tipo == 4:  # Hole Punching
            print(f"[HOLE PUNCHING] Recebido de {addr}")
            # Responde para confirmar conexão
            enviar_pacote(addr, 5, b'')  # Tipo 5: Hole Punching ACK
            
        elif tipo == 5:  # Hole Punching ACK
            print(f"[HOLE PUNCHING] Confirmado com {addr}")
            
        elif tipo == 10:  # Listar Arquivos Request
            # Envia lista de arquivos
            arquivos = listar_arquivos_locais()
            dados = json.dumps(arquivos).encode()
            enviar_pacote(addr, 11, dados)  # Tipo 11: Listar Arquivos Response
            
        elif tipo == 11:  # Listar Arquivos Response
            # Processa lista de arquivos
            try:
                arquivos_remotos = json.loads(conteudo.decode())
                print(f"\n[RECEBIDO] Lista de {len(arquivos_remotos)} arquivos disponíveis:")
                for arq in arquivos_remotos:
                    print(f"  - {arq['nome']} ({arq['tamanho']} bytes, modificado em {time.ctime(arq['timestamp'])})")
            except Exception as e:
                print(f"[ERRO] Falha ao processar lista de arquivos: {e}")
                
        elif tipo == 12:  # Solicitar Arquivo
            # Processa solicitação de arquivo
            try:
                nome_arquivo = conteudo.decode()
                caminho = os.path.join(PASTA, nome_arquivo)
                
                if os.path.exists(caminho) and os.path.isfile(caminho):
                    print(f"[SOLICITAÇÃO] Enviando arquivo '{nome_arquivo}'")
                    iniciar_envio_arquivo(addr, nome_arquivo, caminho)
                else:
                    enviar_pacote(addr, 14, f"Arquivo '{nome_arquivo}' não encontrado".encode())  # Tipo 14: Erro
            except Exception as e:
                print(f"[ERRO] Falha ao processar solicitação de arquivo: {e}")
                
        elif tipo == 13:  # Início de Transferência
            # Processa início de transferência
            try:
                info = json.loads(conteudo.decode())
                nome_arquivo = info['nome']
                tamanho = info['tamanho']
                hash_valor = info['hash']
                timestamp = info['timestamp']
                total_chunks = info['chunks']
                
                print(f"[TRANSFERÊNCIA] Iniciando recebimento de '{nome_arquivo}' ({tamanho} bytes, {total_chunks} chunks)")
                
                # Verifica se já temos este arquivo e se o recebido é mais recente (last write wins)
                if nome_arquivo in arquivos_info:
                    if timestamp <= arquivos_info[nome_arquivo].timestamp:
                        print(f"[IGNORADO] Já temos uma versão mais recente de '{nome_arquivo}'")
                        enviar_pacote(addr, 14, f"Já existe versão mais recente de '{nome_arquivo}'".encode())
                        return
                
                # Verifica se já recebemos este arquivo recentemente
                agora = time.time()
                if hash_valor in arquivos_recentes:
                    if agora - arquivos_recentes[hash_valor] < 300:  # 5 minutos
                        print(f"[DUPLICADO] Arquivo com hash {hash_valor} já recebido recentemente")
                        enviar_pacote(addr, 14, f"Arquivo duplicado com hash {hash_valor}".encode())
                        return
                
                # Cria estrutura para receber o arquivo
                if addr not in transfer_queues:
                    transfer_queues[addr] = queue.Queue()
                    transfer_threads[addr] = threading.Thread(
                        target=processar_transferencia,
                        args=(addr,),
                        daemon=True
                    )
                    transfer_threads[addr].start()
                
                # Adiciona informações de transferência à fila
                transfer_info = TransferInfo(nome_arquivo, tamanho, hash_valor, timestamp, total_chunks)
                transfer_queues[addr].put(('INIT', transfer_info))
                
                # Envia ACK
                enviar_pacote(addr, 15, nome_arquivo.encode())  # Tipo 15: ACK
                
            except Exception as e:
                print(f"[ERRO] Falha ao iniciar recebimento: {e}")
                
        elif tipo == 14:  # Erro
            # Processa mensagem de erro
            try:
                mensagem = conteudo.decode()
                print(f"[ERRO REMOTO] {mensagem}")
            except:
                print(f"[ERRO REMOTO] Mensagem de erro ilegível")
                
        elif tipo == 15:  # ACK
            # Processa confirmação
            try:
                mensagem = conteudo.decode()
                print(f"[ACK] {mensagem}")
            except:
                print(f"[ACK] Recebido")
                
        elif tipo == 16:  # Chunk de Arquivo
            # Processa chunk de arquivo
            try:
                if len(conteudo) < 12:
                    return
                
                nome_arquivo_len = struct.unpack('!H', conteudo[:2])[0]
                nome_arquivo = conteudo[2:2+nome_arquivo_len].decode()
                chunk_idx = struct.unpack('!I', conteudo[2+nome_arquivo_len:6+nome_arquivo_len])[0]
                chunk_size = struct.unpack('!I', conteudo[6+nome_arquivo_len:10+nome_arquivo_len])[0]
                chunk_data = conteudo[10+nome_arquivo_len:10+nome_arquivo_len+chunk_size]
                
                # Adiciona chunk à fila de processamento
                if addr in transfer_queues:
                    transfer_queues[addr].put(('CHUNK', (nome_arquivo, chunk_idx, chunk_data)))
                    
                    # Envia ACK para o chunk
                    ack_data = struct.pack('!HI', len(nome_arquivo.encode()), chunk_idx) + nome_arquivo.encode()
                    enviar_pacote(addr, 17, ack_data)  # Tipo 17: Chunk ACK
                
            except Exception as e:
                print(f"[ERRO] Falha ao processar chunk: {e}")
                
        elif tipo == 17:  # Chunk ACK
            # Processa confirmação de chunk
            try:
                nome_arquivo_len = struct.unpack('!H', conteudo[:2])[0]
                chunk_idx = struct.unpack('!I', conteudo[2:6])[0]
                nome_arquivo = conteudo[6:6+nome_arquivo_len].decode()
                
                print(f"[CHUNK ACK] {nome_arquivo} chunk {chunk_idx}")
                
                # Aqui você pode implementar lógica para reenvio de chunks perdidos
                # ou para controle de fluxo
                
            except Exception as e:
                print(f"[ERRO] Falha ao processar ACK de chunk: {e}")
                
        elif tipo == 18:  # Fim de Transferência
            # Processa fim de transferência
            try:
                nome_arquivo = conteudo.decode()
                print(f"[FIM TRANSFERÊNCIA] Arquivo '{nome_arquivo}' transferido com sucesso")
                
                # Aqui você pode implementar lógica para verificar se todos os chunks foram recebidos
                
            except Exception as e:
                print(f"[ERRO] Falha ao processar fim de transferência: {e}")
                
    except Exception as e:
        print(f"[ERRO] Falha ao processar pacote: {e}")

def processar_transferencia(addr):
    """
    Processa a fila de transferência para um peer
    """
    transfer_info = None
    
    while running:
        try:
            if addr not in transfer_queues:
                break
                
            item = transfer_queues[addr].get(timeout=1)
            if not item:
                continue
                
            tipo, dados = item
            
            if tipo == 'INIT':
                transfer_info = dados
                
            elif tipo == 'CHUNK' and transfer_info:
                nome_arquivo, chunk_idx, chunk_data = dados
                
                if nome_arquivo != transfer_info.nome_arquivo:
                    continue
                    
                # Verifica se este chunk já foi recebido
                if chunk_idx in transfer_info.chunks_recebidos:
                    continue
                    
                # Calcula a posição no buffer
                offset = chunk_idx * CHUNK_SIZE
                if offset + len(chunk_data) <= transfer_info.tamanho:
                    transfer_info.buffer[offset:offset+len(chunk_data)] = chunk_data
                    transfer_info.chunks_recebidos.add(chunk_idx)
                    
                    # Verifica se recebemos todos os chunks
                    if len(transfer_info.chunks_recebidos) == transfer_info.total_chunks:
                        # Verifica integridade
                        hash_calculado = calcular_hash(transfer_info.buffer)
                        if hash_calculado == transfer_info.hash:
                            # Salva arquivo
                            caminho_arquivo = os.path.join(PASTA, transfer_info.nome_arquivo)
                            with open(caminho_arquivo, "wb") as f:
                                f.write(transfer_info.buffer)
                            print(f"[RECEBIDO] Arquivo '{transfer_info.nome_arquivo}' verificado e salvo.")
                            
                            # Atualiza informações locais
                            arquivos_info[transfer_info.nome_arquivo] = ArquivoInfo(
                                transfer_info.nome_arquivo, 
                                transfer_info.timestamp, 
                                transfer_info.hash, 
                                transfer_info.tamanho
                            )
                            
                            # Registra o hash como recentemente recebido
                            arquivos_recentes[transfer_info.hash] = time.time()
                            
                            # Publica na DHT
                            executar_na_dht(publicar_arquivo_na_dht(
                                transfer_info.nome_arquivo, 
                                transfer_info.timestamp, 
                                transfer_info.hash
                            ))
                            
                            # Propaga para outros peers (protocolo gossip)
                            propagar_arquivo(transfer_info.nome_arquivo, caminho_arquivo, addr)
                            
                            # Limpa a transferência
                            transfer_info = None
                        else:
                            print(f"[ERRO] Verificação falhou para '{transfer_info.nome_arquivo}'")
                            print(f"Hash esperado: {transfer_info.hash}")
                            print(f"Hash calculado: {hash_calculado}")
                            transfer_info = None
            
            transfer_queues[addr].task_done()
            
        except queue.Empty:
            continue
        except Exception as e:
            print(f"[ERRO] Falha no processamento de transferência: {e}")
            if transfer_info:
                transfer_info = None

def receber_pacotes():
    """
    Recebe e processa pacotes UDP
    """
    global running
    
    while running:
        try:
            dados, addr = udp_socket.recvfrom(MAX_PACKET_SIZE)
            thread_pool.submit(processar_pacote, dados, addr)
        except Exception as e:
            print(f"[ERRO RECEPÇÃO] {e}")

def iniciar_envio_arquivo(addr, nome_arquivo, caminho):
    """
    Inicia o envio de um arquivo para um peer
    """
    try:
        tamanho = os.path.getsize(caminho)
        hash_arquivo = calcular_hash_arquivo(caminho)
        timestamp = time.time()
        
        # Calcula o número total de chunks
        total_chunks = (tamanho + CHUNK_SIZE - 1) // CHUNK_SIZE
        
        # Envia informações de início de transferência
        info = {
            'nome': nome_arquivo,
            'tamanho': tamanho,
            'hash': hash_arquivo,
            'timestamp': timestamp,
            'chunks': total_chunks
        }
        
        enviar_pacote(addr, 13, json.dumps(info).encode())  # Tipo 13: Início de Transferência
        
        # Inicia thread para envio de chunks
        thread = threading.Thread(
            target=enviar_chunks_arquivo,
            args=(addr, nome_arquivo, caminho, total_chunks),
            daemon=True
        )
        thread.start()
        
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao iniciar envio de '{nome_arquivo}': {e}")
        return False

def enviar_chunks_arquivo(addr, nome_arquivo, caminho, total_chunks):
    """
    Envia chunks de um arquivo para um peer
    """
    try:
        nome_bytes = nome_arquivo.encode()
        nome_len = len(nome_bytes)
        
        with open(caminho, "rb") as f:
            for i in range(total_chunks):
                # Lê o chunk do arquivo
                f.seek(i * CHUNK_SIZE)
                chunk_data = f.read(CHUNK_SIZE)
                
                # Prepara o cabeçalho do chunk
                header = struct.pack('!HII', nome_len, i, len(chunk_data))
                
                # Envia o chunk
                enviar_pacote(addr, 16, header + nome_bytes + chunk_data)  # Tipo 16: Chunk de Arquivo
                
                # Espera um pouco para controle de fluxo
                time.sleep(0.01)
        
        # Envia mensagem de fim de transferência
        enviar_pacote(addr, 18, nome_arquivo.encode())  # Tipo 18: Fim de Transferência
        
        print(f"[ENVIADO] '{nome_arquivo}' enviado com sucesso")
        
        # Atualiza informações locais
        tamanho = os.path.getsize(caminho)
        hash_arquivo = calcular_hash_arquivo(caminho)
        timestamp = time.time()
        arquivos_info[nome_arquivo] = ArquivoInfo(nome_arquivo, timestamp, hash_arquivo, tamanho)
        
        # Publica na DHT
        executar_na_dht(publicar_arquivo_na_dht(nome_arquivo, timestamp, hash_arquivo))
        
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao enviar chunks de '{nome_arquivo}': {e}")
        return False

def propagar_arquivo(nome_arquivo, caminho, origem=None):
    """
    Propaga o arquivo para um subconjunto de peers usando protocolo gossip
    """
    # Seleciona um subconjunto aleatório de peers (log N)
    num_peers = len(peers)
    if num_peers <= 1:
        return
        
    # Logaritmo na base 2 do número de peers, no mínimo 3
    num_targets = max(3, int(math.log2(num_peers)))
    
    # Seleciona peers aleatórios
    peer_addrs = list(peers.keys())
    if origem in peer_addrs:
        peer_addrs.remove(origem)
        
    if not peer_addrs:
        return
        
    targets = random.sample(peer_addrs, min(num_targets, len(peer_addrs)))
    
    for addr in targets:
        try:
            if peers[addr].active:
                iniciar_envio_arquivo(addr, nome_arquivo, caminho)
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

def solicitar_lista_arquivos(addr):
    """
    Solicita lista de arquivos de um peer
    """
    try:
        enviar_pacote(addr, 10, b'')  # Tipo 10: Listar Arquivos Request
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao solicitar lista de arquivos: {e}")
        return False

def solicitar_arquivo(addr, nome_arquivo):
    """
    Solicita um arquivo específico de um peer
    """
    try:
        enviar_pacote(addr, 12, nome_arquivo.encode())  # Tipo 12: Solicitar Arquivo
        return True
    except Exception as e:
        print(f"[ERRO] Falha ao solicitar arquivo: {e}")
        return False

def manter_conexoes_ativas():
    """
    Envia heartbeats periódicos para manter conexões ativas
    """
    global running
    
    while running:
        for addr, peer in list(peers.items()):
            if peer.active:
                enviar_heartbeat(addr)
        time.sleep(30)  # Envia a cada 30 segundos

def tentar_hole_punching_periodico():
    """
    Tenta hole punching periodicamente para peers não conectados
    """
    global running
    
    while running:
        for addr, peer in list(peers.items()):
            if peer.active and peer.ext_addr:
                iniciar_hole_punching(peer.ext_addr)
        time.sleep(RETRY_INTERVAL)

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

def connect_to_peer(ip, porta):
    """
    Conecta a um peer usando UDP e hole punching
    """
    try:
        addr = (ip, porta)
        
        # Adiciona o peer à lista
        if addr not in peers:
            peers[addr] = PeerInfo(addr)
            print(f"[NOVO PEER] {addr}")
        
        # Envia solicitação de discovery
        enviar_discovery(addr)
        
        # Tenta hole punching direto (pode não funcionar se ambos estiverem atrás de NAT)
        enviar_pacote(addr, 4, b'')  # Tipo 4: Hole Punching
        
        return True
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python p2p_udp.py <porta>")
        print("Porta Recomendada: 5000")
        sys.exit(1)

    PORTA_LOCAL = int(sys.argv[1])

    if not os.path.exists(PASTA):
        os.makedirs(PASTA)
        print(f"[CRIADO] Pasta '{NOME_PASTA}' criada em: {PASTA}")
    else:
        print(f"[EXISTE] Pasta encontrada: {PASTA}")

    # Inicia o socket UDP
    iniciar_udp_socket(PORTA_LOCAL)
    
    # Obtém o endereço externo
    get_external_ip()
    
    # Inicia o loop DHT em uma thread separada
    dht_thread = threading.Thread(target=iniciar_dht_loop, daemon=True)
    dht_thread.start()
    time.sleep(1)  # Espera o DHT inicializar

    # Inicia thread para receber pacotes
    recv_thread = threading.Thread(target=receber_pacotes, daemon=True)
    recv_thread.start()
    
    # Inicia thread para manter conexões ativas
    threading.Thread(target=manter_conexoes_ativas, daemon=True).start()
    
    # Inicia thread para tentar hole punching periodicamente
    threading.Thread(target=tentar_hole_punching_periodico, daemon=True).start()
    
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
            print("6. Mostrar peers conectados")
            print("7. Sair")
            escolha = input("Escolha uma opção: ")

            if escolha == "1":
                ip = input("IP do peer: ")
                porta = int(input("Porta do peer: "))
                if connect_to_peer(ip, porta):
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
                if not peers:
                    print("Não há peers conectados.")
                else:
                    print("Peers conectados:")
                    for i, addr in enumerate(peers.keys()):
                        print(f"{i+1}. {addr[0]}:{addr[1]}")
                    
                    try:
                        idx = int(input("Escolha um peer (número): ")) - 1
                        if 0 <= idx < len(peers):
                            addr = list(peers.keys())[idx]
                            solicitar_lista_arquivos(addr)
                        else:
                            print("Índice inválido.")
                    except ValueError:
                        print("Entrada inválida.")
            
            elif escolha == "4":
                if not peers:
                    print("Não há peers conectados.")
                else:
                    print("Peers conectados:")
                    for i, addr in enumerate(peers.keys()):
                        print(f"{i+1}. {addr[0]}:{addr[1]}")
                    
                    try:
                        idx = int(input("Escolha um peer (número): ")) - 1
                        if 0 <= idx < len(peers):
                            addr = list(peers.keys())[idx]
                            nome_arquivo = input("Nome do arquivo a solicitar: ")
                            solicitar_arquivo(addr, nome_arquivo)
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
                print(f"\nPeers conectados ({len(peers)}):")
                for addr, peer in peers.items():
                    ext_addr = f"{peer.ext_addr[0]}:{peer.ext_addr[1]}" if peer.ext_addr else "Desconhecido"
                    print(f"  - {addr[0]}:{addr[1]} (Endereço externo: {ext_addr}, Última atividade: {time.ctime(peer.last_seen)})")
            
            elif escolha == "7":
                print("Encerrando...")
                break
            
            else:
                print("Opção inválida.")
    
    except KeyboardInterrupt:
        print("\nEncerrando...")
    
    finally:
        # Limpa recursos
        running = False
        observer.stop()
        observer.join()
        
        try:
            udp_socket.close()
        except:
            pass
        
        thread_pool.shutdown(wait=False)
