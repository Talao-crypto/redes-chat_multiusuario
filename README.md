# Projeto Prático 1 — Chat Multiusuário

Projeto desenvolvido para a disciplina de **Redes de Computadores** do curso de **Engenharia de Computação**.

## Sobre o projeto

O projeto consiste no desenvolvimento de uma aplicação de **chat cliente/servidor**, utilizando comunicação através de **Sockets** e execução concorrente com **Threads**.

Nesta primeira etapa, o projeto terá **apenas um usuário**, seguindo a especificação fornecida para o Projeto Prático 1.

O objetivo principal é colocar em prática conceitos de:

- Comunicação em rede
- Modelo Cliente/Servidor
- Sockets
- Threads
- Comunicação bidirecional assíncrona
- Memória compartilhada
- Estruturas de dados

## Funcionamento

A aplicação será dividida em duas partes:

### Cliente

O cliente será responsável por:

- Conectar-se ao servidor;
- Receber a mensagem inicial de conexão;
- Permitir que o usuário digite mensagens e comandos;
- Enviar os dados para o servidor;
- Receber mensagens e informações enviadas pelo servidor.

O cliente utilizará **duas threads**:

- **Thread 1:** leitura do teclado e envio de dados;
- **Thread 2:** recebimento e exibição de dados do servidor.

### Servidor

O servidor será responsável por:

- Aceitar conexões dos clientes;
- Receber mensagens e comandos;
- Armazenar as informações em uma estrutura compartilhada;
- Processar as ações solicitadas;
- Enviar mensagens aos clientes;
- Enviar data e horário periodicamente.

O servidor também utilizará **duas threads**:

- **Thread 1:** recebimento dos dados enviados pelo cliente;
- **Thread 2:** processamento das informações e envio de dados.

## Comandos

### Mensagem

Qualquer texto que **não comece com `:`** será considerado uma mensagem.

```text
Olá, tudo bem?

tales

