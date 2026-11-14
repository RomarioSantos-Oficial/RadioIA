# Relatório de proposta — Sistema de playout para OBS

**Status:** aprovado pelo usu?rio; implementa??o validada em teste automatizado isolado em 14/11/2026. O teste n?o incluiu opera??o real no OBS.

## 1. Objetivo

Criar um programa local para organizar e automatizar a reprodução de vídeos e áudios usados em uma programação de rádio/TV, com uma tela de operação e uma saída de reprodução que possa ser adicionada ao OBS Studio como Fonte de Navegador.

Neste projeto, “leitura” significa carregar e reproduzir os arquivos de mídia. O `inicio.md` não descreve transcrição, teleprompter ou leitura de texto por voz; essas funções não fazem parte desta proposta.

## 2. O que o programa permitirá fazer

- Cadastrar pastas locais e identificar arquivos `.mp4`, `.webm`, `.mov`, `.mp3`, `.wav` e `.ogg`, com duração e tipo de mídia.
- Escolher pastas e arquivos específicos em seletores nativos do Windows, sem depender de digitar caminhos.
- Selecionar uma pasta raiz que contenha todos os programas; salvar nela o banco e as configurações do playout.
- Organizar arquivos em programas ou blocos, alterar a ordem, ativar/desativar itens e configurar reprodução única, repetição limitada ou loop.
- Criar uma grade semanal com dia, hora de início e fim, duração/limite opcional e modo de transição: corte na hora marcada ou espera pelo fim da mídia atual.
- Criar blocos de comerciais com duração prevista e uma sequência própria de anúncios, vinhetas ou outros arquivos.
- Reunir no mesmo programa arquivos de várias pastas e identificar categorias de música, vinheta, patrocinador e comercial, com inserção configurável a cada quantidade de músicas.
- Importar uma raiz com vários programas, vídeos e bibliotecas de vinhetas/comerciais próprias ou compartilhadas; organizar a ordem dos programas e distribuir o tempo com proporção-alvo 90/10.
- Criar pastas de programa e importar arquivos escolhidos pelo operador, copiando-os para as pastas de Vídeos, Vinhetas e Comerciais correspondentes.
- Repetir a sequência do programa durante o horário agendado, recomeçando a fila se os arquivos terminarem antes do horário final.
- Acompanhar no painel o relógio, o conteúdo no ar, o progresso e o estado da automação; iniciar, pausar, avançar ou parar a reprodução.
- Escolher execução sequencial ou aleatória por programa, avançar automaticamente após cada arquivo e exibir a duração total das mídias e a duração reservada para programas e comerciais.
- Reproduzir a programação na rota `/player`, em tela cheia e sem controles visíveis, para uso como Browser Source no OBS. Áudios podem manter uma imagem de fundo ou logotipo.
- Salvar a biblioteca e a programação localmente para continuar disponíveis ao reiniciar o programa.

## 3. Fluxo de uso proposto

1. O operador cadastra uma pasta e importa suas mídias.
2. Organiza as mídias em programas/blocos e define ordem, repetição e limites.
3. Monta a grade semanal, incluindo os horários de comerciais.
4. Inicia o motor e confere o conteúdo atual pelo painel `/admin`.
5. Adiciona `http://localhost:8000/player` como Fonte de Navegador no OBS.

## 4. Componentes e tecnologias

- **Servidor e motor:** Python 3.10+, FastAPI e Uvicorn.
- **Dados:** SQLite para biblioteca, programas, itens, configurações e grade.
- **Agendamento e comunicação:** APScheduler e WebSockets para enviar atualizações e comandos ao player.
- **Leitura de duração:** FFprobe, por atender áudio e vídeo. Será necessário instalá-lo ou incluí-lo/configurá-lo junto ao sistema.
- **Interface:** HTML, JavaScript e TailwindCSS, com painel `/admin` e player `/player`.
- **Inicialização no Windows:** arquivo `.bat` e instruções de configuração do OBS.

## 5. Regras operacionais propostas

- O sistema usa o relógio e o fuso horário configurados no computador.
- Eventos sobrepostos na grade serão sinalizados e não poderão ser ativados até o conflito ser resolvido.
- O horário final do bloco encerra a reprodução daquele bloco; um evento seguinte pode começar conforme a regra de corte ou espera configurada.
- Um intervalo comercial terá duração-alvo. A proposta é tocar os anúncios em sequência e, ao atingir o limite do intervalo, seguir a grade; isso pode cortar um anúncio que ultrapasse o horário. Se o intervalo terminar antes do limite, o sistema pode tocar uma vinheta ou imagem/áudio de espera até o próximo bloco.
- A interface informará quando uma pasta ou arquivo não estiver mais acessível, para que o operador possa corrigir o caminho ou reimportar a pasta.
- A saída do player depende do OBS manter a Fonte de Navegador ativa e de o navegador permitir iniciar o áudio. A primeira reprodução pode exigir um clique de ativação, conforme o próprio guia.

## 6. Etapas de implementação sugeridas

1. Estrutura do projeto, banco SQLite e importação/varredura de pastas.
2. Player web para OBS e controles de reprodução, fila e repetição.
3. Grade semanal, blocos de programas, limites de tempo e transições.
4. Intervalos comerciais e gestão da sequência de anúncios.
5. Painel operacional, inicializador do Windows e instruções de uso.
6. Revisão dos requisitos do `inicio.md` e dos requisitos de comerciais antes de considerar o sistema pronto para operação.

## 7. Pontos para aprovação

1. **Comerciais:** aplicar duração-alvo e encerrar um anúncio no fim do intervalo.
2. **Leitura de pastas:** incluir subpastas na varredura, com opção para desativar esse comportamento.
3. **Uso inicial:** considerar um computador Windows com OBS local.

## 8. Limites desta proposta

O desenho é para playout local integrado ao OBS. O guia não especifica redundância de servidor, operação remota, distribuição de sinal ou garantias de disponibilidade de uma emissora de transmissão contínua. Também será necessário validar no ambiente real os codecs dos arquivos e as configurações de áudio/vídeo do OBS.

## 9. Base consultada

Proposta elaborada a partir do [inicio.md](inicio.md) e do pedido de incluir organização de programas, sequência/duração e espaços para comerciais.

## 10. Valida??o automatizada

Em 14/11/2026, a aplica??o foi iniciada em uma porta local tempor?ria e testada com arquivos de m?dia de exemplo, usando uma pasta tempor?ria separada dos dados da emissora. Todos os cen?rios abaixo passaram:

- Criar um fluxo em uma raiz vazia e criar as pastas V?deos, Vinhetas e Comerciais.
- Copiar e registrar os arquivos selecionados, reconhecer as pastas no plural e ocultar pastas internas da lista de programas avulsos.
- Gerar a fila com an?ncios sequenciais na propor??o-alvo 90/10.
- Salvar um hor?rio, iniciar o fluxo pelo agendador e rejeitar conflitos de hor?rio.
- Entregar m?dia ao player, avan?ar pelo evento `ended` mesmo depois de uma pausa e validar a sintaxe JavaScript das duas p?ginas.

A verifica??o confirma as rotas e a l?gica local do aplicativo. A apar?ncia e o ?udio de uma transmiss?o real no OBS ainda dependem de valida??o no computador da emissora.
