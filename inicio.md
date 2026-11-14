Blueprint e Guia de Prompts: Sistema de Playout e Automação (Áudio e Vídeo para OBS)

Este documento foi estruturado para que você possa utilizar qualquer Inteligência Artificial de programação (como Claude, ChatGPT, Cursor, Windsurf ou Gemini) para construir, testar e validar o seu sistema de reprodução automatizada de áudios e vídeos.

1. Visão Geral da Arquitetura (O que será construído)

Para garantir estabilidade, sincronismo de áudio/vídeo e integração direta com o OBS Studio sem precisar capturar a tela do computador, o sistema será dividido em duas telas alimentadas por um motor local:

Backend e Motor de Automação (Python + FastAPI + SQLite):

Lê pastas locais escolhidas por você no computador.

Identifica vídeos (.mp4, .webm, .mov) e áudios (.mp3, .wav, .ogg) e extrai a duração exata de cada arquivo.

Controla o relógio da programação (grade horária estilo Rádio/TV).

Envia comandos em tempo real via WebSockets para o reprodutor.

Painel de Controle (/admin):

Interface web local onde você escolhe as pastas, ordena os arquivos, ativa/desativa o Loop individual de cada mídia, define limites de tempo e monta a grade de horários.

Tela de Saída / Player (/player):

Uma página limpa, sem botões ou bordas, feita exclusivamente para ser adicionada como Fonte de Navegador (Browser Source) no OBS Studio.

Reproduz vídeos em tela cheia e, quando toca apenas música/áudio, exibe uma imagem de fundo padrão ou logotipo configurado por você.

2. Passo a Passo de Como Guiar a IA

Nunca peça para uma IA criar um sistema inteiro de uma só vez, pois ela tende a simplificar funções importantes. Siga exatamente esta ordem:

Passo 1: Crie uma pasta vazia no seu computador chamada sistema-playout.

Passo 2: Abra sua IA de preferência (ou editor com IA, como Cursor/VS Code) e envie o Prompt 0 (Contexto Base).

Passo 3: Envie o Prompt 1 e peça para testar a leitura de pastas.

Passo 4: Envie o Prompt 2 para criar o Player compatível com OBS e a lógica de Loop individual.

Passo 5: Envie o Prompt 3 para implementar a Grade de Programação (horários de rádio/TV).

Passo 6: Envie o Prompt 4 para construir o Painel Visual de Controle.

Passo 7: Utilize os Prompts de Verificação (Seção 4) para obrigar a IA a revisar o próprio código e corrigir falhas antes de você usar no ar.

3. Prompts de Desenvolvimento (Copiar e Colar)

Prompt 0: Inicialização e Regras do Projeto

Envie este prompt primeiro para alinhar a IA:

Atue como um Engenheiro de Software Sênior especialista em sistemas de Broadcast, Playout de Rádio/TV e automação multimídia.

Vamos construir um sistema local de Playout e Automação de Áudio e Vídeo com integração para o OBS Studio. 

Requisitos obrigatórios do sistema:
1. Leitura de pastas locais escolhidas pelo usuário, listando arquivos de vídeo (.mp4, .webm, .mov) e áudio (.mp3, .wav, .ogg).
2. Organização da ordem de reprodução dos arquivos dentro de cada pasta/bloco.
3. Controle de tempo máximo de execução por bloco ou arquivo.
4. Configuração individual por arquivo: cada mídia pode ter "Loop Infinito", "Repetir N vezes" ou "Tocar 1 vez".
5. Agendamento (Grade Horária estilo Rádio/TV): capacidade de programar blocos por dia da semana e hora exata (HH:MM:SS), podendo escolher entre "Corte Seco" (interrompe o que está tocando na hora marcada) ou "Transição Suave" (aguarda o arquivo atual terminar ou encerra o loop atual antes de mudar).
6. Saída dedicada para OBS Studio via URL local (/player) usando HTML5, controlada em tempo real via WebSockets pelo servidor Python.

Stack tecnológica obrigatória:
- Backend: Python 3.10+, FastAPI, Uvicorn, SQLite (via SQLAlchemy ou sqlite3 nativo), APScheduler e WebSockets.
- Leitura de metadados de mídia: Mutagen ou FFprobe.
- Frontend: HTML5, JavaScript moderno e TailwindCSS (via CDN para facilitar execução local sem build complexo).

Antes de escrever qualquer código, crie apenas a estrutura de pastas e arquivos recomendada para esse projeto e me explique brevemente como cada arquivo vai se comunicar. Aguarde minha confirmação para começarmos a Etapa 1.


Prompt 1: Backend, Banco de Dados e Leitor de Pastas

Envie após aprovar a estrutura do Prompt 0:

Agora vamos implementar a Etapa 1: Banco de Dados e Leitor de Pastas (Scanner).

Crie o código completo para:
1. Banco de dados SQLite contendo as tabelas:
   - `pastas_programas`: id, nome, caminho_local, tempo_limite_segundos (opcional), modo_reproducao (sequencial/aleatorio).
   - `midias`: id, pasta_id, nome_arquivo, caminho_completo, tipo (audio/video), duracao_segundos, ordem, loop_infinito (booleano), repetir_vezes (inteiro, padrão 1), ativo (booleano).
   - `grade_horaria`: id, nome_evento, pasta_id, dias_semana (ex: "0,1,2,3,4,5,6"), hora_inicio (HH:MM:SS), hora_fim (HH:MM:SS), corte_seco (booleano), ativo (booleano).
2. Uma função de escaneamento de pasta que:
   - Recebe o caminho de uma pasta no Windows/Linux.
   - Lê todos os arquivos de áudio e vídeo suportados.
   - Extrai a duração real de cada arquivo em segundos.
   - Salva no banco preservando a ordem e permitindo reordenar depois.
3. Endpoints da API FastAPI para cadastrar uma pasta, reescanear uma pasta, atualizar a ordem dos arquivos e alterar as configurações de loop de um arquivo específico.
4. Um endpoint de streaming de mídia (`/media/{midia_id}`) usando `FileResponse` para que o navegador consiga reproduzir arquivos locais de qualquer diretório do computador com suporte a HTTP Range Requests.

Forneça o código completo e funcional desta etapa, junto com o arquivo `requirements.txt`.


Prompt 2: O Player para OBS e Lógica de Loop Individual

Envie após testar o Prompt 1:

Agora vamos implementar a Etapa 2: O Player de Saída para OBS (`/player`) e o Motor de Fila em Tempo Real.

Crie o sistema de WebSockets no backend e a página `player.html` com os seguintes requisitos críticos:
1. A página `/player` deve ter fundo preto (ou imagem de fundo padrão configurável), ocupar 100% da tela (`100vw`, `100vh`), sem barras de rolagem, sem botões visíveis e com cursor oculto.
2. Deve usar um elemento `<video>` principal que reproduza tanto vídeos quanto áudios. Se o arquivo atual for apenas áudio, deve exibir uma imagem de fundo/capa padrão enquanto toca o som.
3. Lógica de Fila e Loop no Backend:
   - O servidor mantém o estado atual (`midia_atual`, `contador_repeticoes`, `tempo_decorrido_bloco`, `modo_automatico`).
   - Quando o `<video>` dispara o evento `ended` (terminou de tocar), ele avisa o servidor via WebSocket.
   - O servidor verifica: se `loop_infinito` for verdadeiro (e o tempo limite do bloco não tiver estourado), manda tocar o mesmo arquivo novamente. Se `repetir_vezes > 1`, incrementa o contador e repete até atingir o limite. Caso contrário, avança para o próximo arquivo ativo na ordem definida.
4. Inclua um botão discreto de "Iniciar Motor de Áudio" que some após o primeiro clique (ou suporte a autoplay desmutado para o OBS Browser Source).

Forneça o código atualizado do servidor e do `player.html`.


Prompt 3: Agendador de Programação (Grade Horária de Rádio/TV)

Envie após validar que o Player toca e faz loops corretamente:

Agora vamos implementar a Etapa 3: O Agendador de Programação (Scheduler estilo Rádio/TV).

Integre o `APScheduler` (ou um loop assíncrono de precisão de 1 segundo no FastAPI) com as seguintes regras:
1. O agendador verifica continuamente a tabela `grade_horaria` comparando com o dia da semana e hora atual do sistema.
2. Quando chegar a `hora_inicio` de um programa agendado:
   - Se `corte_seco == True`: interrompe imediatamente o arquivo ou loop que está no ar no `/player`, carrega a playlist da pasta agendada e inicia a reprodução do item 1 no mesmo segundo.
   - Se `corte_seco == False`: marca uma flag `proximo_bloco_pendente`. Assim que o arquivo atual terminar (mesmo que estivesse em loop infinito, o loop é encerrado ao fim da execução atual), o sistema inicia o novo bloco agendado.
3. Controle de `hora_fim` / Tempo Determinado pelo Usuário:
   - Se um bloco ou pasta tiver um tempo limite definido (ex: 30 minutos) ou `hora_fim`, ao atingir esse tempo o sistema deve encerrar o bloco atual e passar para o próximo item da grade ou entrar em modo de espera/vinheta padrão.
4. Crie endpoints na API para criar, editar, listar e excluir eventos da grade horária.

Forneça o código completo dessa integração.


Prompt 4: Painel de Controle Visual (/admin)

Envie para construir a interface onde você vai operar o sistema:

Agora vamos implementar a Etapa 4: O Painel de Controle do Operador (`/admin`).

Crie uma interface web moderna, escura (dark mode, estilo estúdio de rádio/TV) e intuitiva em `admin.html` contendo:
1. **Monitoramento Ao Vivo (Topo):**
   - Relógio digital grande em tempo real.
   - Status do que está tocando agora no `/player` (nome do arquivo, barra de progresso, tempo restante e status do loop).
   - Botões de controle manual: Play, Pause, Pular para Próximo, Parar Tudo e chave de "Automação Ativa / Manual".
2. **Gerenciador de Pastas e Arquivos:**
   - Campo para colar o caminho da pasta do computador e botão "Importar Pasta".
   - Lista de arquivos da pasta selecionada permitindo:
     * Reordenar arquivos (botões Subir/Descer ou Drag-and-Drop).
     * Marcar/Desmarcar checkbox de **Loop Infinito** por arquivo.
     * Definir número de repetições por arquivo.
     * Botão "Tocar Agora" para disparar um arquivo avulso imediatamente.
3. **Grade de Programação (Agendador):**
   - Formulário simples para agendar uma pasta: escolher pasta, dias da semana, hora de início, hora de fim e escolher entre "Corte Seco" ou "Aguardar término atual".
   - Tabela mostrando a programação do dia em ordem cronológica.

Garanta que todas as ações no painel atualizem o banco de dados via API e reflitam em tempo real via WebSocket.


Prompt 5: Facilitador de Inicialização (Execução com 1 Clique)

Envie ao final para facilitar o uso diário:

Crie dois arquivos auxiliares para facilitar o uso no dia a dia:
1. Um arquivo `iniciar_sistema.bat` (para Windows) que ativa o ambiente virtual Python (se existir), instala dependências ausentes se necessário, inicia o servidor FastAPI na porta 8000 e abre automaticamente o navegador no endereço `http://localhost:8000/admin`.
2. Um arquivo `README.md` simples explicando onde colar o link `http://localhost:8000/player` dentro do OBS Studio e como configurar o áudio.


4. Prompts de Auditoria e Verificação (Checklist da Ideia)

Sempre que a IA terminar uma etapa (ou ao final de todo o projeto), cole o prompt abaixo para forçar a IA a auditar o próprio código e garantir que nada da sua ideia original ficou de fora ou com bugs ocultos:

Prompt de Verificação Geral (Auditoria de Requisitos)

Faça uma auditoria rigorosa no código que acabamos de criar comparando com os requisitos originais do projeto. Verifique cada um dos 7 pontos abaixo e me responda com "OK" ou "CORRIGIR" (já aplicando a correção no código se encontrar falhas):

1. [Leitura de Pastas]: O sistema lê corretamente tanto vídeos quanto áudios de pastas locais com espaços e acentos no nome do caminho?
2. [Ordem Customizada]: Se o usuário mudar a ordem dos arquivos no painel, a fila de reprodução respeita exatamente a nova ordem salva no banco?
3. [Loop Individual vs. Tempo]: Se um arquivo estiver marcado com "Loop Infinito", ele realmente repete sem travar o navegador? E se chegar o horário de um programa agendado ou acabar o tempo determinado pelo usuário, o sistema consegue quebrar esse loop infinito para iniciar o próximo programa?
4. [Áudio sem Vídeo no OBS]: Quando toca um arquivo de áudio (.mp3/.wav), a tela `/player` exibe uma imagem visual limpa em vez de mostrar um ícone de erro de vídeo quebrado?
5. [Transição sem Buracos]: Quando um arquivo acaba e o próximo começa, a transição ocorre automaticamente sem precisar de interação manual?
6. [Corte Seco vs. Suave]: O agendador diferencia corretamente quando deve cortar um vídeo no meio (hora exata) e quando deve esperar o vídeo atual acabar?
7. [Sincronismo WebSocket]: Se o OBS Studio perder a conexão momentaneamente ou recarregar a Fonte de Navegador, o `/player` reconecta sozinho ao WebSocket e puxa o que está tocando no momento?


5. Como Conectar no OBS Studio (Configuração Recomendada)

Depois que o sistema estiver rodando, configure o OBS Studio seguindo estes passos para obter qualidade máxima de áudio e vídeo:

No OBS Studio, vá no painel Fontes e clique no botão + > Navegador (Browser).

Dê o nome de Playout Automacao e clique em OK.

Preencha as configurações da janela:

URL: http://localhost:8000/player

Largura: 1920 (ou a resolução base da sua transmissão)

Altura: 1080

Marque a caixa: Controlar áudio via OBS (Isso fará o som dos vídeos e músicas aparecer como uma barra independente no Mixer de Áudio do OBS, permitindo ajustar volume, colocar filtros ou mutar).

Desmarque a caixa: Desativar fonte quando não estiver visível (Importante para que a programação continue rodando no tempo certo mesmo se você trocar de cena no OBS).

Clique em OK. Se o navegador bloquear o som automático na primeira vez, clique com o botão direito na fonte Playout Automacao > Interagir e dê um clique na tela.