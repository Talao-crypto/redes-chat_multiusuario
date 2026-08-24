import socket

HOST = "127.0.0.1"
PORT = 5000

cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#cria o socket com IPv4 e TCP

cliente.connect((HOST, PORT)) #tenta conectar no servidor 127.0.0.1:5000

print("Conectado ao servidor!")

mensagem = input("Digite uma mensagem: ") 

cliente.sendall(mensagem.encode())
#sendall envia para o server
#encode e so para transformar em bytes

resposta = cliente.recv(1024)
#espera resposta

print(f"Servidor respondeu: {resposta.decode()}")

cliente.close()