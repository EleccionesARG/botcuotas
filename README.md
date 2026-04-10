# pulso-agent

Agente de control de cuotas + Meta Ads para Pulso Research.  
Monitorea Firebase en tiempo real y ejecuta acciones en Meta Ads con confirmación via Telegram.

## Variables de entorno (Railway)

| Variable       | Valor                                      |
|----------------|--------------------------------------------|
| `TG_TOKEN`     | Token del bot de Telegram                  |
| `TG_CHAT_ID`   | Tu chat ID de Telegram                     |
| `META_TOKEN`   | Access token de Meta Ads                   |
| `META_ACCOUNT` | ID de la cuenta publicitaria (sin `act_`)  |
| `FB_API_KEY`   | API Key de Firebase                        |
| `FB_DB_URL`    | URL de Firebase Realtime Database          |

## Lógica de acciones

### Adset 16-29
- Cuota completa en **ambos géneros** → **Pausa** el adset
- Cuota completa en **un solo género** → **Separa** en dos adsets (M/F) y pausa el completado

### Adset +30
- Cuota 50-64 completa en ambos → **Acota** a 30-49
- Cuota 30-49 completa en ambos → **Pausa** el adset

Todas las acciones requieren **confirmación via Telegram** antes de ejecutarse.

## Convención de nombres de adsets

`Estrato X 16-29` / `Estrato X +30`

Ejemplos: `Estrato 0 16-29`, `Estrato 1 +30`, `Estrato 2 16-29`

## Deploy

1. Nuevo repo en GitHub → `pulso-agent`
2. Railway → New Project → Deploy from GitHub
3. Cargar las 6 variables de entorno
4. Al deployar, el agente manda un mensaje de inicio a Telegram
