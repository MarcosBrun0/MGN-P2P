import socket
import threading
import sys
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
from kademlia.network import Server as DHTServer
import asyncio

conexoes_ativas = []

NOME_PASTA = "shared_folder"
PASTA = os.path.join(os.getcwd(), NOME_PASTA)

class FileChangeHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return

        caminho = event.src_path
        nome_arquivo = os.path.basename(caminho)

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

        try:
            with open(caminho, "rb") as f:
                conteudo = f.read()
        except Exception as e:
            print(f"[ERRO] Falha ao ler '{nome_arquivo}': {e}")
            return

        if not conteudo:
            print(f"[AVISO] Arquivo '{nome_arquivo}' está vazio. Ignorando envio.")
            return

        mensagem = f"[ARQUIVO]{nome_arquivo}||".encode() + conteudo
        print(f"[NOVO ARQUIVO] Enviando '{nome_arquivo}' aos peers...")

        for conn in conexoes_ativas:
            try:
                conn.sendall(mensagem)
            except Exception as e:
                print(f"[ERRO] Falha ao enviar para um peer: {e}")
        
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)

        for conn in conexoes_ativas:
            try:
                peer_ip, peer_port = conn.getpeername()
                new_loop.run_until_complete(dht_node.set(nome_arquivo, f"{peer_ip}:{peer_port}"))
            except Exception as e:
                print(f"[ERRO] DHT set falhou: {e}")

        new_loop.close()

def iniciar_monitoramento(pasta):
    event_handler = FileChangeHandler()
    observer = Observer()
    observer.schedule(event_handler, path=pasta, recursive=False)
    observer.start()
    print(f"[MONITORANDO] Pasta: {pasta}")
    return observer

def handle_client(client_socket):
    while True:
        try:
            data = client_socket.recv(4096)

            if not data:
                break

            if data.startswith(b"[ARQUIVO]"):
                header, conteudo = data.split(b"||", 1)
                nome_arquivo = header.decode().replace("[ARQUIVO]", "")
                caminho_arquivo = os.path.join(PASTA, nome_arquivo)

                with open(caminho_arquivo, "wb") as f:
                    f.write(conteudo)

                print(f"\n[RECEBIDO] Arquivo '{nome_arquivo}' salvo.")
            else:
                print(f"\n[MENSAGEM] {data.decode()}")
        except Exception as e:
            print(f"[ERRO] Falha no cliente: {e}")
            break

    client_socket.close()

def start_server(port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(('0.0.0.0', port))
    server.listen()
    print(f"[SERVIDOR] Escutando na porta {port}...")

    while True:
        client, addr = server.accept()
        print(f"[NOVA CONEXÃO] {addr}")
        conexoes_ativas.append(client)
        thread = threading.Thread(target=handle_client, args=(client,))
        thread.daemon = True
        thread.start()

def connect_to_peer(ip, port):
    try:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((ip, port))
        conexoes_ativas.append(client)
        print(f"[CONECTADO] ao peer {ip}:{port}")
        thread = threading.Thread(target=handle_client, args=(client,))
        thread.daemon = True
        thread.start()
        return client
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python p2p.py <porta>")
        print("Porta Recomendada:5000")
        sys.exit(1)

    PORTA_LOCAL = int(sys.argv[1])

    if not os.path.exists(PASTA):
        os.makedirs(PASTA)
        print(f"[CRIADO] Pasta '{NOME_PASTA}' criada em: {PASTA}")
    else:
        print(f"[EXISTE] Pasta encontrada: {PASTA}")

    # --- Inicializa DHT ---
    loop = asyncio.get_event_loop()
    dht_node = DHTServer()
    loop.run_until_complete(dht_node.listen(PORTA_LOCAL + 1000))  # Porta DHT separada

    # Primeira vez: não tem ninguém
    if len(sys.argv) >= 4:
        bootstrap_ip = sys.argv[2]
        bootstrap_port = int(sys.argv[3])
        loop.run_until_complete(dht_node.bootstrap([(bootstrap_ip, bootstrap_port + 1000)]))

    threading.Thread(target=start_server, args=(PORTA_LOCAL,), daemon=True).start()
    observer = iniciar_monitoramento(PASTA)

    try:
        while True:
            print("\n--- MENU ---")
            print("1. Conectar a outro peer")
            print("2. Procurar arquivo")
            print("3. Sair")
            escolha = input("Escolha uma opção: ")

            if escolha == "1":
                ip = input("IP do peer: ")
                porta = int(input("Porta do peer: "))
                connect_to_peer(ip, porta)
                # Bootstrapping do DHT
                loop.run_until_complete(dht_node.bootstrap([(ip, porta + 1000)]))
            elif escolha == "2":
                nome_arquivo = input("Nome do arquivo para buscar: ")
                resultado = loop.run_until_complete(dht_node.get(nome_arquivo))
                if resultado:
                    print(f"[ENCONTRADO] Peer com '{nome_arquivo}': {resultado}")
                else:
                    print("[NÃO ENCONTRADO] Nenhum peer possui esse arquivo.")
            elif escolha == "3":
                print("Encerrando...")
                break
            else:
                print("Opção inválida.")
    finally:
        observer.stop()
        observer.join()
