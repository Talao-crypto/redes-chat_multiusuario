import socket

HOST = "127.0.0.1"  #definindo a porta e o endereço 
PORT = 5000

servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#socket do servidor
#AD_INET (IPv4)
#SOCK_STREAM (TCP)

servidor.bind((HOST, PORT)) #servidor = 127.0.0.1:5000
servidor.listen(1) #modo de receber (ouvir)

print("Servidor iniciado")
print("Aguardando conexão...")

conexao, endereco = servidor.accept() 

print(f"Cliente conectado: {endereco}")

mensagem = conexao.recv(1024)   #recebe os dados (1024 bytes)

print(f"Mensagem recebida: {mensagem.decode()}")

conexao.sendall("Mensagem recebida pelo servidor!".encode())    #mostra uma resposta

conexao.close()
servidor.close()
#fechar servidor