# Playout para OBS

Aplicativo local para organizar arquivos de áudio e vídeo, montar programas e intervalos comerciais e reproduzir uma grade automatizada no OBS Studio.

## Iniciar no Windows

1. Instale Python 3.10 ou superior.
2. Instale FFmpeg e confirme que `ffprobe` está disponível no `PATH` do Windows. O playout usa o FFprobe para identificar duração e tipo dos arquivos.
3. Execute `iniciar_sistema.bat`. Na primeira execução, ele cria um ambiente Python local e instala as dependências.
4. O painel abre em [http://localhost:8000/admin](http://localhost:8000/admin). Mantenha a janela do servidor aberta enquanto estiver usando o sistema.

Escolha a pasta raiz da programação no painel. O banco SQLite e as configurações são salvos nessa raiz, em `.playout/playout.sqlite3`. O caminho escolhido fica registrado para o próximo início do aplicativo. Os arquivos de mídia originais permanecem nas pastas escolhidas; o sistema salva seus caminhos e configurações, sem copiá-los nem apagá-los.

## Fluxo de vários programas

Use **Importar fluxo** e escolha a raiz que guarda os programas. Estruturas aceitas incluem:

```text
Radio/
  Programas/
    Jornal da manhã/
      Videos/
      Vinhetas/
      Comerciais/
    Programa esportivo/
      Videos/
      Vinhetas/
      Comerciais/
```

Também são aceitas pastas `Vinhetas` e `Comerciais` compartilhadas na raiz. Cada subpasta em `Programas` vira um programa; use as setas para ordenar e desmarque **Incluir** para deixar um programa fora do fluxo. Escolha ordem sequencial ou aleatória para os vídeos. Os anúncios são reproduzidos em sequência.

Para criar um programa pelo painel, informe o nome e clique em **Criar pastas do programa**. O playout cria `Videos`, `Vinhetas` e `Comerciais` dentro da raiz do fluxo. Os botões **Vídeos +**, **Vinhetas +** e **Comerciais +** abrem o seletor de arquivos e copiam os itens escolhidos para a pasta correspondente.

O fluxo usa como padrão 90% do tempo para vídeos dos programas e 10% para vinhetas/comerciais; ajuste **Tempo destinado aos vídeos principais (%)** para escolher outra proporção. A ordem aleatória embaralha os vídeos dentro de cada programa, mantendo vinhetas e comerciais nos intervalos. O campo **Duração de cada intervalo** define o tamanho do bloco de anúncios; a proporção determina quanto tempo de vídeo há entre os blocos. Com 90/10, por exemplo, um bloco de 10 minutos de anúncios é inserido aproximadamente a cada 90 minutos de vídeo. Os intervalos são inseridos entre arquivos, por isso a proporção final pode variar ligeiramente conforme a duração das mídias. Agende o fluxo na grade usando a origem **Fluxo de programas**; ele avança ao próximo programa e repete o ciclo até o fim do horário.

## Escolher mídias e montar programas

- Use **Escolher pasta…** para selecionar uma pasta. A opção **Incluir subpastas** controla a varredura recursiva.
- Use **Adicionar outra pasta…** para reunir, no mesmo programa, mídias de pastas separadas, como `Músicas`, `Vinhetas` e `Patrocinadores`.
- Use **Escolher arquivos…** para adicionar arquivos específicos ao programa selecionado. Se nenhum programa estiver selecionado, o sistema cria o grupo “Arquivos selecionados”.
- Escolha **Ordem aleatória** para embaralhar as músicas ou deixe desmarcado para seguir a ordem salva na lista. Ao terminar um arquivo, o player avança automaticamente.
- São aceitos `.mp4`, `.webm`, `.mov`, `.mp3`, `.wav` e `.ogg` que o FFmpeg consiga ler.
- As pastas chamadas `Vinhetas`, `Patrocinadores` ou `Comerciais` são identificadas automaticamente. Também é possível escolher a categoria de cada arquivo na tabela.
- Configure uma vinheta, patrocínio ou comercial para entrar a cada quantidade definida de músicas. Use `0` para desativar a inserção programada; os arquivos dessa categoria ainda entram no fim de cada sequência.
- Na biblioteca, mude a ordem, desative itens, escolha reprodução única, repetição limitada ou loop infinito e toque um item ou programa.
- O painel mostra o tempo total das mídias ativas e o total de todos os arquivos da biblioteca. Na grade, também mostra o tempo reservado para o bloco e o tempo somado dos arquivos escolhidos.
- O limite de duração de um programa manual também vale para a sessão iniciada pelo botão **Tocar programa**.

## Programação e comerciais

Crie eventos por dia da semana, com horários de início e fim. Um fim anterior ao início representa um evento que atravessa a meia-noite. Eventos com horários sobrepostos são recusados para evitar disputas pela saída.

No formulário, escolha **Fluxo de programas** como origem para tocar a raiz importada. O fluxo passa pelas pastas de programas na ordem configurada; vídeos e anúncios avançam automaticamente. Também é possível agendar uma playlist única ou um intervalo comercial.

Em eventos do tipo **Programa**, todos os arquivos ativos formam uma sequência. Se ela terminar antes do fim do horário, o sistema reinicia a sequência e continua até o limite do evento ou do programa. A opção de ordem aleatória embaralha as músicas; vinhetas, patrocínios e comerciais entram nos intervalos configurados.

Para um intervalo comercial, escolha o programa/playlist que contém os anúncios e defina o horário do intervalo. O player toca os arquivos ativos em ordem e encerra o bloco no horário final; se um anúncio ultrapassar esse limite, ele será cortado para liberar o próximo horário. A programação automática só começa quando **Ativar** estiver ligado no painel.

Na transição **Aguardar fim da mídia atual**, o próximo evento espera a mídia atual terminar. O horário final do evento continua sendo respeitado; se o bloco pendente expirar antes da transição, ele não será iniciado.

## Configurar o OBS Studio

1. Em **Fontes**, clique em **+ > Navegador**.
2. Use o endereço `http://localhost:8000/player` e a resolução base da sua transmissão, por exemplo 1920 × 1080.
3. Marque **Controlar áudio via OBS** para controlar a saída pelo Mixer de áudio.
4. Para manter a programação ao trocar de cena, desmarque **Desativar fonte quando não estiver visível**.
5. Se o áudio não iniciar, abra **Interagir** na fonte do navegador e clique em **Clique para iniciar o áudio**.

O painel e a rota de mídia escutam apenas em `127.0.0.1`; são destinados ao uso no mesmo computador do OBS.
#
