import socket
import threading
import queue
from datetime import datetime

HOST = "127.0.0.1"  # definindo a porta e o endereço
PORT = 5051


def hora_atual():
    # Retorna a hora atual como string no formato HH:MM:SS
    return datetime.now().strftime("%H:%M:%S")


def enviar(conexao, lock, texto):
    # Envia `texto` para o socket `conexao` usando `lock` para
    # evitar que múltiplas threads enviem ao mesmo tempo.
    with lock:
        conexao.sendall(texto.encode())


def thread_recepcao(conexao, endereco, fila, evento_sair):
    # Nome inicial do usuário baseado no IP:porta; pode ser alterado
    # pelo comando `:nome <novo>` enviado pelo cliente.
    nome_usuario = f"{endereco[0]}:{endereco[1]}"  # nome padrão

    # Loop que recebe dados do socket e traduz em eventos colocados
    # na `fila` para o thread de processamento tratar.
    while not evento_sair.is_set():
        try:
            dados = conexao.recv(1024)
        except OSError:
            # Problema no recv: sinaliza encerramento da conexão
            fila.put({"tipo": "comando_quit", "conteudo": "", "usuario": nome_usuario})
            break

        # conexão fechada pelo cliente
        if not dados:
            fila.put({"tipo": "comando_quit", "conteudo": "", "usuario": nome_usuario})
            break

        texto = dados.decode(errors="replace").strip()

        # Comandos especiais reconhecidos pelo servidor:
        if texto.startswith(":nome "):
            # Atualiza nome localmente e envia evento para processamento
            nome_usuario = texto[len(":nome "):].strip()
            fila.put({"tipo": "comando_nome", "conteudo": nome_usuario, "usuario": nome_usuario})
        elif texto.startswith(":quit"):
            # Cliente pediu para sair
            fila.put({"tipo": "comando_quit", "conteudo": "", "usuario": nome_usuario})
            break
        else:
            # Mensagem normal: será formatada e enviada de volta
            fila.put({"tipo": "mensagem", "conteudo": texto, "usuario": nome_usuario})


def thread_processamento(conexao, fila, evento_sair, lock_envio):
    # Consome itens da fila e reage conforme o tipo:
    # - `mensagem`: formata e envia ao cliente
    # - `comando_nome`: confirma alteração
    # - `comando_quit`: encerra a conexão
    while not evento_sair.is_set():
        item = fila.get()  # bloqueia até ter algo

        try:
            if item["tipo"] == "mensagem":
                # Formata a mensagem com o nome e hora, e envia.
                texto_formatado = f"{item['usuario']} ({hora_atual()}): {item['conteudo']}\n"
                # Eco para o remetente
                eco = f"Voce digitou: {item['conteudo']}\n"
                enviar(conexao, lock_envio, texto_formatado)
                enviar(conexao, lock_envio, eco)

            elif item["tipo"] == "comando_nome":
                # Confirma a mudança de nome ao cliente
                enviar(conexao, lock_envio, f"Nome alterado para: {item['conteudo']}\n")

            elif item["tipo"] == "comando_quit":
                # Cliente encerrou: tenta avisar e fecha tudo localmente
                try:
                    enviar(conexao, lock_envio, "Encerrando conexao...\n")
                except OSError:
                    pass
                evento_sair.set()
                conexao.close()
                break
        except OSError:
            # Erro no envio: encerra o loop de processamento
            evento_sair.set()
            break


def thread_relogio(conexao, evento_sair, lock_envio, intervalo=60):
    # Envia a hora atual periodicamente para o mesmo cliente enquanto
    # a conexão estiver ativa.
    while not evento_sair.is_set():
        interrompido = evento_sair.wait(timeout=intervalo)
        if interrompido:
            break
        try:
            enviar(conexao, lock_envio, hora_atual() + "\n")
        except OSError:
            evento_sair.set()
            break


def main():
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind((HOST, PORT))
    servidor.listen(1)

    print("Servidor iniciado")
    print("Aguardando conexão...")

    # Aceita apenas UMA conexão: o `accept()` é chamado uma única vez.
    # Para múltiplos clientes, seria necessário um loop que chama
    # `accept()` repetidamente e cria estruturas por cliente.
    conexao, endereco = servidor.accept()
    print(f"Cliente conectado: {endereco}")

    # Aviso de conexão e estruturas de sincronização/coordenação
    enviar(conexao, threading.Lock(), f"{hora_atual()}: CONECTADO!!\n")

    fila = queue.Queue()
    evento_sair = threading.Event()
    lock_envio = threading.Lock()

    # Três threads colaborando para lidar com a conexão:
    # - recepção dos dados,
    # - processamento (respostas/eco),
    # - envio periódico do relógio.
    t1 = threading.Thread(target=thread_recepcao, args=(conexao, endereco, fila, evento_sair))
    t2 = threading.Thread(target=thread_processamento, args=(conexao, fila, evento_sair, lock_envio))
    t3 = threading.Thread(target=thread_relogio, args=(conexao, evento_sair, lock_envio))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()

    conexao.close()
    servidor.close()
    print("Servidor encerrado")


if __name__ == "__main__":
    main()
