import socket
import threading
import sys
import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

conexoes_ativas = []

class FileChangeHandler(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory:
            return

        tipo = 'modificado'
        if event.event_type == 'created':
            tipo = 'criado'
        elif event.event_type == 'deleted':
            tipo = 'removido'

        caminho = os.path.basename(event.src_path)
        mensagem = f"[ALTERAÇÃO] Arquivo {tipo}: {caminho}"
        print(mensagem)

        for conn in conexoes_ativas:
            try:
                conn.send(mensagem.encode())
            except:
                pass

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
            data = client_socket.recv(1024).decode()
            if not data:
                break
            print(f"\n[Recebido] {data}")
        except:
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
        return client
    except Exception as e:
        print(f"[ERRO] Não foi possível conectar: {e}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python p2p.py <porta>")
        sys.exit(1)

    PORTA_LOCAL = int(sys.argv[1])

    # Verificar/criar pasta compartilhada
    NOME_PASTA = "shared_folder"
    PASTA = os.path.join(os.getcwd(), NOME_PASTA)

    if not os.path.exists(PASTA):
        os.makedirs(PASTA)
        print(f"[CRIADO] Pasta '{NOME_PASTA}' criada em: {PASTA}")
    else:
        print(f"[EXISTE] Pasta encontrada: {PASTA}")

    # Iniciar servidor e monitoramento
    threading.Thread(target=start_server, args=(PORTA_LOCAL,), daemon=True).start()
    observer = iniciar_monitoramento(PASTA)

    try:
        while True:
            print("\n--- MENU ---")
            print("1. Conectar a outro peer")
            print("2. Sair")
            escolha = input("Escolha uma opção: ")

            if escolha == "1":
                ip = input("IP do peer: ")
                porta = int(input("Porta do peer: "))
                connect_to_peer(ip, porta)
            elif escolha == "2":
                print("Encerrando...")
                break
            else:
                print("Opção inválida.")
    finally:
        observer.stop()
        observer.join()
