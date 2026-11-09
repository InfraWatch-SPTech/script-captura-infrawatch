# Cópias dos scripts de captura e leitura

Esta pasta contém cópias dos três arquivos analisados. Os originais em `script-captura-infrawatch` permanecem intactos.

- `script-leitura-hardware.py`: mantém o processamento existente e acrescenta a geração de `dash-previsibilidade_*.json` em Silver e Gold. O novo resumo consulta até 30 dias de arquivos Bronze do equipamento, agrega CPU/RAM/disco/rede/temperatura quando presentes e calcula estatísticas, séries por janela, tendências e runway quando há histórico e limites suficientes.
- `Scripts/captura.py`: mantém a captura existente, amplia o ciclo para 15 amostras e habilita a sonda HTTP por padrão. O alvo inicial é `https://httpbin.org/post`, usando POST. Faz chamadas em níveis de concorrência 1, 2, 4, 8 e 16 (repetidos três vezes), com 20 chamadas por nível, e registra volume, erros, throughput e latências. Pode ser desabilitada explicitamente com `INFRAWATCH_REQUEST_PROBE_ENABLED=false` ou apontada para um endpoint da aplicação em staging com `INFRAWATCH_REQUEST_PROBE_URL` e `INFRAWATCH_REQUEST_PROBE_METHOD`.
- `script-leitura-processos.py`: cópia sem alterações funcionais nesta etapa.

O alvo padrão `httpbin.org` é um substituto temporário para simular o endpoint do cliente enquanto a URL real não está disponível. Latência e throughput incluem a rede e o serviço de teste; quando houver um endpoint do cliente em staging, informe-o em `INFRAWATCH_REQUEST_PROBE_URL`. Cada execução envia até 300 requisições ao alvo selecionado.

Os CSVs atuais não incluem limites de alerta nem eventos de indisponibilidade; esses campos ficam indisponíveis no novo resumo até serem capturados ou fornecidos por outra fonte. A previsão só é calculada com pelo menos três amostras por recurso; com menos, o arquivo contém a quantidade encontrada e a mensagem para exibição no site.

Na dashboard, a seleção do servidor consulta a rota existente `/hardwares/buscarEqId/:idEquipamento`, que valida usuário/empresa/permissão e retorna os limites reais cadastrados. O aviso de amostras aceita o resumo ETL via `window.atualizarStatusAmostrasPrevisibilidade(payload)`; o backend ainda não expõe os objetos Gold/Silver do S3, então por enquanto a página avisa que a contagem real não está conectada e mantém os gráficos simulados.
