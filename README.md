# Покупка последнего билета

Учебное event-driven демо на Go: один концерт, одно место, билет за **500 ₽**. Четыре сервиса, отдельные SQLite, настоящая Kafka. Страница покупателя создаёт заказ; страница ведущего показывает события, попытки, деньги, процессы и позиции групп.

## Запуск

Нужны Docker Engine и Docker Compose v2. Из корня проекта:

```bash
docker compose up --build -d --wait
```

- Покупатель: <http://localhost:8080/>
- Ведущий: <http://localhost:8080/presenter>

Первый запуск подготавливает обычную покупку. Дождитесь «Готово», затем нажмите **«Купить за 500 ₽»** у покупателя. Сборка выполняет Go-тесты; первый запуск скачивает закреплённые образы и модули.

Для другого порта: `DEMO_PORT=18080 docker compose up --build -d --wait`. Оба экрана доступны только через loopback. Это локальный учебный стенд без production-аутентификации; не публикуйте его через туннель.

Остановка с сохранением данных:

```bash
docker compose down
```

Повторный запуск восстанавливает тот же прогон и желаемые состояния процессов. Новый чистый стенд без удаления существующих данных:

```bash
DEMO_PORT=18090 docker compose -p last-ticket-go-cleancheck up --build -d --wait
```

Имя `last-ticket-go-cleancheck` должно быть новым для вашей машины. Это отдельные volumes; исходный `last-ticket-go-demo` и старый `last-ticket-demo` сохраняются.

## Пять опытов

У ведущего выберите подготовку, дождитесь «Готово», затем купите билет на странице покупателя. Подготовка сама не создаёт заказы.

| Кнопка подготовки | Что наблюдать | Продолжение |
|---|---|---|
| Обычная покупка | OrderCreated → PaymentSucceeded → SeatReserved; билет, один CHARGE | Не требуется |
| Приостановить бронирование | Оплата сохранена, PaymentSucceeded уже в Kafka, заказ ждёт | Возобновить бронирование |
| Два покупателя, одно место | Первый клик создаёт два постоянных задания; один билет, один возврат | Не требуется |
| Сбой платежей сразу после списания | Consumer действительно выходит с кодом 73 после COMMIT БД; offset не зафиксирован | Восстановить платежи |
| Приостановить отправитель событий | Заказ и PENDING outbox есть, OrderCreated ещё не опубликован | Возобновить отправитель |

После восстановления платежей сравните **1 запись OrderCreated, 2 попытки, 1 списание**. Попытки имеют одинаковые topic/partition/offset и разные worker_instance_id. Карточки заказов, денежный журнал, inbox/outbox и offsets читаются из настоящих источников. Неизвестные позиции обозначены «—», ошибка источника видна отдельно.

Покупатель сохраняет ключ запроса и order_id в localStorage: перезагрузка и потерянный HTTP-ответ повторяют прежнюю покупку. Ведущий автоматически выбирает заказ покупателя в том же браузерном профиле; можно вручную выбрать другой заказ или все.

**«Сбросить демо»** создаёт новый run_id и увеличивает generation через QUIESCE → INITIALIZE → START → OPEN. История БД, топики и offsets не удаляются. Старый pending outbox становится ABANDONED; корректные старые события получают IGNORED_RUN. При ошибке подготовки покупки остаются закрыты: повторите ту же операцию кнопкой ведущего.

## Презентация

Проверенная версия **presenterm 0.16.1**, 12 смысловых слайдов / 33 кадра раскрытия:

```bash
presenterm --config-file slides/presenterm.yaml slides/talk.md
```

Нужен графический терминал (Kitty, Ghostty, iTerm2 или WezTerm). Если определение протокола не сработало, добавьте `--image-protocol kitty-local` либо `--image-protocol iterm2`. PNG содержат шрифты, сеть для показа не нужна. При отключённом цвете используйте `env -u NO_COLOR presenterm ...`.

Space/→ — дальше; ← — назад; `gg` — начало; `G` — конец; `q` — выход. Переходы: **14G** — покупка, **22G** — пауза, **24G** — гонка, **27G** — повтор. Режим без раскрытий: `presenterm --config-file slides/presenterm.yaml slides/final.md`.

[Заметки докладчика](slides/speaker-notes.md) · [Протокол проверки](slides/verification.md)

```bash
python3 scripts/check_slides.py
# Только для пересборки картинок; нужны librsvg/rsvg-convert и fontconfig:
python3 scripts/build_slides.py
```

## Тесты

Для тестов на хосте нужны **Go 1.26.11**, CGO/C-компилятор и `goimports` в PATH. Go-зависимости закреплены в go.mod/go.sum. Обычный `go test` не требует Docker, Kafka, Chrome или Node.js:

```bash
make test
make lint
go test -race ./...
go mod verify
```

Приёмка использует отдельный project **last-ticket-go-acceptance**, HTTP 18080, внутренние тестовые порты 18081–18083 и Kafka 19092 (все loopback). Она останавливает Kafka и перезапускает тестовые сервисы. Не запускайте её одновременно с ручной демонстрацией на этом project:

```bash
DEMO_PORT=18080 docker compose -p last-ticket-go-acceptance -f compose.yaml -f compose.test.yaml up --build -d --wait
DEMO_BASE_URL=http://localhost:18080 DEMO_COMPOSE_PROJECT=last-ticket-go-acceptance \
  go test -tags=integration ./tests/integration -count=1 -v
```

Повреждённое событие проверяется на отдельном пустом broker, поскольку оно намеренно блокирует группу:

```bash
TEST_KAFKA_PORT=19093 docker compose -p last-ticket-go-protocol -f compose.yaml -f compose.test.yaml up -d --wait kafka kafka-init
PROTOCOL_KAFKA_BROKER=localhost:19093 go test -tags=integration ./tests/integration -run TestIsolatedPoisonConsumer -count=1 -v
```

Браузерные тесты используют Playwright 1.56.1 и Chrome с временным профилем. Для приложения эти зависимости не нужны:

```bash
npm install --prefix /tmp/last-ticket-browser --cache /tmp/last-ticket-npm-cache --no-audit --no-fund playwright@1.56.1
node tests/go-browser.cjs
node tests/go-browser-state.cjs
```

По умолчанию — `http://localhost:18080`, Playwright из `/tmp/last-ticket-browser/node_modules/playwright`, Chrome `/Applications/Google Chrome.app/Contents/MacOS/Google Chrome`. Настройки: `DEMO_BASE_URL`, `PLAYWRIGHT_MODULE`, `CHROME_PATH`. Выполняйте тесты последовательно: они меняют текущий прогон. Скриншоты и тайминги пишутся в `artifacts/go-*`; при ошибке Go-интеграции сохраняют диагностический снимок.

[Матрица T01–T25 и фактические результаты](docs/ACCEPTANCE.md). Автоматические секунды браузера не заменяют устную репетицию и проверку проектора.

## Устройство и версии

| Сервис / consumer group | Подписки |
|---|---|
| orders / demo.orders | payments.events, reservations.events |
| payments / demo.payments | orders.events, reservations.events |
| reservations / demo.reservations | payments.events |
| demo-web / demo.journal | Все три топика, независимый физический журнал |

Две партиции на топик; record key — order_id. Заказ и outbox сохраняются до ответа 202. Эффект, inbox и исходящий outbox атомарны в локальной транзакции. Commit Kafka выполняется после неё, auto commit отключён. Издатель повторяет неизменные байты и event_id, помечает SENT после ack. Списание и возврат — отдельные операции; уникальные бизнес-ключи защищают от повторного эффекта. Проекция накапливает факты и не откатывает финальный статус от совместимых поздних событий.

Server управляет настоящими дочерними consumer/publisher, ждёт их завершения и фиксирует exit code. Session timeout Kafka — 6 с, heartbeat — 2 с, rebalance timeout — 60 с. В franz-go нет Java-параметра `max.poll.interval.ms`; обработка ограничена контекстами и `BlockRebalanceOnPoll`/`AllowRebalance`.

| Компонент | Версия |
|---|---|
| Go | 1.26.11 |
| Kafka | 4.1.2 |
| franz-go / kmsg | 1.22.3 / 1.15.1 |
| go-sqlite3 | 1.14.53 |
| presenterm | 0.16.1 |

Образы закреплены digest в Dockerfile/Compose. SQLite: WAL, synchronous=FULL, foreign_keys=ON, busy_timeout=5000, immediate transactions, одно подключение на процесс. Сервисы читают чужие данные только через API. Внутренние порты публикует исключительно тестовый override.

[Техническая спецификация](docs/DEMO_TECHNICAL_SPEC.md) · [Описание презентации](docs/PRESENTATION_DESCRIPTION.md). Предыдущий Python runtime, его тесты и старый README сохранены в `legacy/python/`; основной запуск от них не зависит. Старые artifacts без префикса `go-` относятся к прежней реализации.
