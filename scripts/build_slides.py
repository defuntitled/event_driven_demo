#!/usr/bin/env python3
"""Build the authored SVG/PNG frames and presenterm decks; no network access."""

from html import escape
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
ASSETS = ROOT / "slides/assets"
BG, CARD, FG, MUTED = "#10151D", "#1C2533", "#F5F7FA", "#B7C2D3"
LINE, BLUE, GREEN, YELLOW, RED = "#77869B", "#70C4FF", "#7AD7A3", "#FFD075", "#FF9292"
TITLES = [
    "Деньги списались. Где билет?", "Кто отвечает за покупку",
    "Покупка через HTTP", "Сервисы реагируют на события",
    "Как Kafka хранит и доставляет события", "Покупаем последний билет",
    "Между SQLite и Kafka", "Transactional Outbox", "Бронирование на паузе",
    "Два покупателя. Одно место", "Две попытки. Одно списание",
    "Что мы знаем о заказе?",
]
COUNTS = [2, 2, 3, 3, 3, 2, 3, 3, 2, 3, 3, 4]
TIMES = ["0:00–1:00", "1:00–3:00", "3:00–6:00", "6:00–7:00",
         "7:00–9:00", "9:00–11:00", "11:00–12:00", "12:00–14:00",
         "14:00–17:00", "17:00–20:00", "20:00–23:00", "23:00–25:00"]


class Slide:
    def __init__(self, number):
        self.number = number
        self.layers = [[], [], [], []]
        self.stage = 0
        if number != 1:
            self.text(96, 119, TITLES[number - 1], size=52, weight=600)

    def add(self, markup):
        self.layers[self.stage].append(markup)

    def text(self, x, y, value, size=30, color=FG, weight=400, mono=False, anchor="start"):
        family = "IBM Plex Mono" if mono else "Inter"
        self.add(f'<text x="{x}" y="{y}" fill="{color}" font-family="{family}" '
                 f'font-size="{size}" font-weight="{weight}" text-anchor="{anchor}">{escape(value)}</text>')

    def lines(self, x, y, values, gap=44, **style):
        for i, value in enumerate(values):
            self.text(x, y + i * gap, value, **style)

    def box(self, x, y, w, h, stroke=LINE, fill=CARD, radius=16, dashed=False):
        dash = ' stroke-dasharray="9 7"' if dashed else ""
        self.add(f'<rect x="{x}" y="{y}" width="{w}" height="{h}" rx="{radius}" '
                 f'fill="{fill}" stroke="{stroke}" stroke-width="2"{dash}/>')

    def path(self, points, color=LINE, arrow=True, dashed=False, width=3):
        d = "M " + " L ".join(f"{x} {y}" for x, y in points)
        marker = f' marker-end="url(#{color[1:]})"' if arrow else ""
        dash = ' stroke-dasharray="9 7"' if dashed else ""
        self.add(f'<path d="{d}" fill="none" stroke="{color}" stroke-width="{width}" '
                 f'stroke-linejoin="round"{marker}{dash}/>')

    def label(self, x, y, value, size=24, color=MUTED, anchor="middle"):
        # A background stroke keeps labels legible when crossing a line.
        self.add(f'<text x="{x}" y="{y}" fill="{color}" font-family="Inter" '
                 f'font-size="{size}" text-anchor="{anchor}" stroke="{BG}" '
                 f'stroke-width="10" paint-order="stroke" stroke-linejoin="round">{escape(value)}</text>')

    def cross(self, x, y):
        self.path([(x-10, y-10), (x+10, y+10)], RED, arrow=False, width=4)
        self.path([(x-10, y+10), (x+10, y-10)], RED, arrow=False, width=4)

    def footer(self, frame):
        return (f'<text x="1504" y="867" fill="{MUTED}" font-family="Inter" '
                f'font-size="24" text-anchor="end">{self.number} / 12</text>')

    def svg(self, frame):
        markers = "".join(f'<marker id="{c[1:]}" viewBox="0 0 10 10" refX="9" refY="5" '
                          f'markerWidth="8" markerHeight="8" orient="auto-start-reverse">'
                          f'<path d="M 1 1 L 9 5 L 1 9" fill="none" stroke="{c}" stroke-width="1.5"/>'
                          '</marker>' for c in (LINE, BLUE, GREEN, YELLOW, RED))
        groups = "".join(f'<g id="reveal-{chr(65+i)}">' + "".join(layer) + '</g>'
                         for i, layer in enumerate(self.layers[:frame+1]))
        return (f'<svg xmlns="http://www.w3.org/2000/svg" width="1600" height="900" '
                f'viewBox="0 0 1600 900" role="img" aria-labelledby="title desc">'
                f'<title id="title">{escape(TITLES[self.number-1])} · {chr(65+frame)}</title>'
                f'<desc id="desc">Слайд {self.number} из 12, кадр {chr(65+frame)}.</desc>'
                f'<defs>{markers}</defs><rect width="1600" height="900" fill="{BG}"/>'
                f'{groups}{self.footer(frame)}</svg>')


def receipt(s, x, y, w=352):
    s.box(x, y, w, 360)
    s.text(x+24, y+55, "Билет на концерт", size=30)
    s.text(x+24, y+142, "500 ₽", size=64, weight=600)
    s.text(x+24, y+224, "Оплата прошла", color=GREEN)
    s.box(x+24, y+263, w-48, 54, stroke=YELLOW, fill=YELLOW, radius=10)
    s.text(x+42, y+300, "В обработке", color=BG, weight=600)


def slide1():
    s = Slide(1)
    s.lines(96, 225, ["Деньги списались.", "Где билет?"], gap=80, size=70, weight=600)
    s.lines(96, 425, ["Event-driven архитектура на примере", "покупки последнего билета"], gap=44)
    receipt(s, 1152, 216)
    s.stage = 1
    s.lines(96, 768, ["Уведомления о списании достаточно,", "чтобы пройти на концерт?"], size=36, gap=46, color=YELLOW)
    return s


def slide2():
    s = Slide(2)
    s.text(96, 210, "1 свободное место · билет 500 ₽", color=MUTED)
    data = [(96, "Заказы", ["Номер заказа", "Статус покупки"], "Заказы и их статусы"),
            (592, "Платежи", ["Списание", "Возврат"], "Денежные операции"),
            (1088, "Бронирование", ["Проверка остатка", "Закрепление места"], "Остаток и резервы")]
    for x, title, lines, db in data:
        s.box(x, 270, 416, 171)
        s.text(x+24, 319, title, weight=600, size=34)
        s.lines(x+24, 368, lines, gap=42)
        s.path([(x+208, 441), (x+208, 499)])
        s.box(x, 504, 416, 126, radius=4)
        s.text(x+24, 550, "SQLite", mono=True, color=BLUE, size=28)
        s.text(x+24, 600, db, size=28)
    s.stage = 1
    s.text(96, 713, "Go 1.26 · Apache Kafka · SQLite · Docker Compose", color=MUTED)
    s.text(96, 795, "Страница покупателя — купить")
    s.text(850, 795, "Страница ведущего — БД и Kafka")
    return s


def slide3():
    s = Slide(3)
    xs = [224, 608, 1008, 1392]
    for x, name in zip(xs, ["Браузер", "Заказы", "Платежи", "Бронирование"]):
        s.box(x-112, 202, 224, 64)
        s.text(x, 244, name, anchor="middle", size=28, weight=600)
        s.path([(x, 268), (x, 652)], arrow=False, width=1, dashed=True)
    for y, a, b, label, reply in [
        (306, 0, 1, "Купить", False), (369, 1, 2, "Списать 500 ₽", False),
        (432, 2, 1, "Оплата прошла", True), (495, 1, 3, "Закрепить место", False),
        (558, 3, 1, "Место закреплено", True), (621, 1, 0, "Билет подтверждён", True),
    ]:
        s.path([(xs[a], y), (xs[b], y)], dashed=reply)
        s.label((xs[a]+xs[b])/2, y-13, label, size=27, color=FG)
    s.stage = 1
    s.cross(953, 432)
    s.label(1180, 430, "Ответ потерян", color=RED)
    s.label(1215, 391, "В БД уже есть списание", size=24, color=GREEN)
    s.box(96, 690, 672, 133, stroke=RED)
    s.text(120, 735, "Тайм-аут", color=RED, weight=600)
    s.text(120, 788, "Безопасно ли повторить списание?", size=29)
    s.stage = 2
    s.label(1392, 297, "Пауза 2 минуты", color=YELLOW)
    s.box(832, 690, 672, 133, stroke=YELLOW)
    s.text(856, 735, "Покупка прервалась", color=YELLOW, weight=600)
    s.text(856, 788, "Кто продолжит после перезапуска?", size=29)
    return s


def slide4():
    s = Slide(4)
    s.lines(96, 214, ["Сервисы публикуют факты о произошедших изменениях.",
                     "Подписчики реагируют на эти факты независимо."], gap=45)
    s.box(96, 342, 304, 112)
    s.text(248, 409, "Покупатель", anchor="middle", weight=600)
    s.box(624, 342, 336, 112)
    s.text(792, 409, "Заказы", anchor="middle", weight=600)
    s.path([(400, 363), (624, 363)], BLUE)
    s.label(512, 337, "Купить · команда", color=BLUE, size=24)
    s.stage = 1
    s.path([(624, 440), (400, 440)], dashed=True)
    s.label(512, 484, "202 Accepted + order_id", size=25)
    s.path([(792, 454), (792, 521), (320, 521), (320, 558)], BLUE)
    s.label(700, 543, "Публикуют", color=BLUE)
    s.box(96, 558, 448, 126, stroke=BLUE)
    s.text(120, 610, "OrderCreated", mono=True, color=BLUE, size=28)
    s.text(120, 657, "Заказ создан")
    s.text(96, 806, "202 Accepted — заказ принят, результат появится позже", color=YELLOW, size=32)
    s.stage = 2
    s.box(640, 558, 320, 126)
    s.text(664, 610, "Платежи", weight=600)
    s.text(664, 657, "Списать 500 ₽")
    s.box(1056, 558, 448, 126, stroke=BLUE)
    s.text(1080, 610, "PaymentSucceeded", mono=True, size=28, color=BLUE)
    s.text(1080, 657, "Оплата прошла")
    s.path([(544, 630), (640, 630)], BLUE)
    s.label(592, 715, "Читают", size=24)
    s.path([(960, 630), (1056, 630)], BLUE)
    s.label(1008, 715, "Публикуют", size=24)
    return s


def slide5():
    s = Slide(5)
    s.text(96, 204, "Kafka хранит потоки записей и позволяет подписчикам читать их независимо", size=30)
    s.box(96, 362, 288, 151)
    s.text(120, 420, "Платежи", weight=600, size=34)
    s.text(120, 468, "producer", mono=True, size=28, color=MUTED)
    s.box(480, 288, 544, 388, stroke=BLUE, fill=BG)
    s.text(504, 336, "Kafka", size=34, weight=600, color=BLUE)
    s.box(504, 362, 496, 211, stroke=BLUE)
    s.text(528, 405, "Топик payments.events", size=28, mono=True)
    for yy, part, nums in [(449, "P0", [40, 41, 42]), (517, "P1", [17, 18, 19])]:
        s.text(528, yy+19, part, size=28, mono=True, color=MUTED)
        for i, num in enumerate(nums):
            x = 605 + i*121
            s.box(x, yy-21, 99, 56, stroke=BLUE if num==42 else LINE, fill=BG, radius=6)
            s.text(x+49.5, yy+17, str(num), anchor="middle", size=28, mono=True,
                   color=BLUE if num==42 else FG)
    s.text(504, 606, "Пример записи", size=24, color=MUTED)
    s.text(504, 638, "P0 · offset 42", size=28, mono=True, color=BLUE)
    s.text(504, 668, "PaymentSucceeded", size=28, mono=True, color=BLUE)
    s.path([(384, 435), (480, 435)], BLUE)
    s.label(432, 346, "Записать", color=BLUE)
    s.stage = 1
    for y, name, offset in [(270, "Бронирование", "42"), (495, "Журнал", "43")]:
        s.box(1120, y, 384, 198)
        s.text(1144, y+44, name, size=30, weight=600)
        s.text(1144, y+88, "consumer group", size=26, mono=True, color=MUTED)
        s.text(1144, y+130, "consumer · P0, P1", size=26, mono=True)
        s.text(1144, y+174, "P0 · далее " + offset, size=28, mono=True, color=YELLOW if offset=="42" else GREEN)
        s.path([(1024, 464), (1060, 464), (1060, y+102), (1120, y+102)], BLUE)
        s.label(1072, y+79, "Читать", color=BLUE, size=23)
    s.stage = 2
    s.lines(96, 774, ["Порядок записей — внутри партиции", "Каждая группа хранит свой прогресс чтения"], gap=44, size=32)
    return s


def slide6():
    s = Slide(6)
    s.text(96, 227, "Ожидаемый путь заказа", color=MUTED, size=24)
    for i, (owner, event) in enumerate([( "Заказы", "OrderCreated"), ("Платежи", "PaymentSucceeded"),
                                       ("Бронирование", "SeatReserved")]):
        y = 263 + i*137
        s.text(96, y+54, owner, size=28)
        s.box(352, y, 560, 88, stroke=BLUE)
        s.text(384, y+55, event, size=30, mono=True, color=BLUE)
        if i<2:
            s.path([(632, y+88), (632, y+129)], BLUE)
    s.text(352, 687, "Билет подтверждён", color=GREEN, size=36, weight=600)
    s.stage = 1
    for y, num, words in [(302, "1", "билет"), (448, "1", "списание"), (594, "0", "свободных мест")]:
        s.text(1040, y+45, num, size=64, color=GREEN, weight=600)
        s.text(1120, y+35, words, size=32)
    s.text(96, 807, "Один order_id связывает историю покупки", size=36)
    return s


def slide7():
    s = Slide(7)
    def row(y, left, ldb, right, rdb, result):
        s.box(96, y, 592, 131)
        s.text(120, y+51, left, size=30)
        s.text(120, y+100, ldb, size=28, mono=True, color=BLUE)
        s.box(912, y, 592, 131)
        s.text(936, y+51, right, size=30)
        s.text(936, y+100, rdb, size=28, mono=True, color=BLUE)
        s.path([(688, y+72), (912, y+72)])
        s.cross(800, y+72)
        s.text(800, y+34, "Сбой", anchor="middle", color=RED)
        s.text(96, y+185, result, color=MUTED)
    row(252, "Сохранить заказ", "SQLite", "Опубликовать OrderCreated", "Kafka",
        "Заказ сохранён · событие ещё не отправлено")
    s.stage = 1
    row(518, "Опубликовать OrderCreated", "Kafka", "Сохранить заказ", "SQLite",
        "Событие опубликовано · заказ ещё не сохранён")
    s.stage = 2
    s.text(96, 813, "Как сохранить заказ и поручение отправить событие атомарно?", size=33, color=YELLOW)
    return s


def slide8():
    s = Slide(8)
    s.box(96, 261, 576, 405, fill=BG)
    s.text(120, 310, "SQLite сервиса заказов", weight=600, size=32)
    s.box(120, 339, 528, 241, stroke=GREEN)
    s.text(144, 383, "Одна транзакция", color=GREEN, size=30)
    s.text(144, 443, "Заказ", size=32)
    s.text(144, 501, "Outbox · OrderCreated", size=28, mono=True, color=BLUE)
    s.text(144, 551, "event_id = evt-101", size=28, mono=True)
    s.text(510, 551, "Пример", size=23, color=MUTED)
    s.text(120, 630, "Обе записи сохранены", color=GREEN)
    s.stage = 1
    s.box(768, 360, 272, 198)
    s.lines(904, 438, ["Отправитель", "outbox"], anchor="middle", weight=600, gap=44)
    s.box(1152, 297, 352, 324, stroke=BLUE)
    s.text(1176, 349, "Kafka", color=BLUE, size=34, weight=600)
    s.text(1176, 401, "orders.events", size=28, mono=True)
    s.box(1176, 447, 304, 83, stroke=BLUE, fill=BG, radius=8)
    s.text(1328, 499, "OrderCreated", size=28, mono=True, color=BLUE, anchor="middle")
    s.path([(648, 493), (708, 493), (708, 403), (768, 403)], BLUE)
    s.label(771, 311, "Читать ожидающее", color=BLUE, size=24)
    s.path([(1040, 414), (1152, 414)], BLUE)
    s.label(1110, 265, "Отправить событие", color=BLUE, size=24)
    s.path([(1152, 551), (1040, 551)], dashed=True)
    s.label(1118, 590, "Подтверждение", size=24)
    s.path([(904, 558), (904, 721), (72, 721), (72, 501), (120, 501)])
    s.label(645, 756, "Отметить отправленным", size=24)
    s.stage = 2
    s.cross(760, 721)
    s.label(1030, 706, "Сбой после подтверждения", color=RED, size=26)
    s.text(96, 819, "Возможна повторная публикация с тем же event_id", color=YELLOW, size=33)
    return s


def slide9():
    s = Slide(9)
    s.box(96, 252, 672, 396)
    s.text(120, 310, "Во время паузы", weight=600, size=34)
    s.lines(120, 384, ["Списание 500 ₽ сохранено", "PaymentSucceeded в Kafka", "Журнал прочитал событие"], gap=62)
    s.text(120, 570, "Бронирование ожидает", color=YELLOW)
    s.text(96, 697, "В обработке", color=YELLOW, size=34, weight=600)
    s.stage = 1
    s.box(912, 252, 592, 396, stroke=GREEN)
    s.text(936, 310, "После восстановления", weight=600, size=34)
    s.lines(936, 384, ["Бронирование прочитало", "событие"], gap=38)
    s.text(936, 489, "SeatReserved", mono=True, size=28, color=BLUE)
    s.text(936, 570, "Билет подтверждён", color=GREEN, size=32)
    s.path([(768, 478), (912, 478)], GREEN)
    s.label(840, 223, "Возобновить", color=GREEN, size=24)
    s.text(96, 771, "Eventual consistency", size=34, color=BLUE)
    s.text(96, 819, "Карточка заказа обновляется после обработки события", size=32)
    return s


def slide10():
    s = Slide(10)
    s.text(96, 197, "Два разных заказа · одно свободное место", color=MUTED)
    s.box(560, 262, 480, 107, stroke=BLUE)
    s.text(800, 307, "PaymentSucceeded", anchor="middle", size=28, mono=True, color=BLUE)
    s.text(800, 347, "Платежи", anchor="middle", size=28)
    s.stage = 1
    s.path([(680, 369), (680, 435), (384, 435), (384, 476)], GREEN)
    s.path([(920, 369), (920, 435), (1192, 435), (1192, 476)], RED)
    s.label(800, 418, "Бронирование атомарно проверяет остаток", color=FG, size=28)
    s.label(384, 458, "Место доступно", color=GREEN, size=24)
    s.label(1192, 458, "Мест нет", color=RED, size=24)
    for x, w, event, label, color in [(96, 576, "SeatReserved", "Бронирование · место закреплено", GREEN),
                                     (880, 624, "SeatReservationFailed", "Бронирование · мест нет", RED)]:
        s.box(x, 476, w, 112, stroke=color)
        s.text(x+24, 522, event, mono=True, color=color, size=28)
        s.text(x+24, 563, label, size=27)
    s.lines(96, 666, ["1 билет · итоговое списание", "500 ₽"], gap=52, size=34, color=GREEN)
    s.stage = 2
    s.path([(1192, 588), (1192, 647)], RED)
    s.label(1338, 626, "Компенсация", color=RED, size=24)
    s.box(880, 651, 624, 108, stroke=GREEN)
    s.text(904, 697, "PaymentRefunded", size=28, mono=True, color=GREEN)
    s.text(904, 738, "Платежи · возврат 500 ₽", size=28)
    s.text(880, 795, "Итоговое списание 0 ₽", color=GREEN, size=28)
    s.text(1504, 197, "Хореография", color=BLUE, anchor="end", size=25)
    s.text(96, 841, "Saga — локальные транзакции и компенсации", size=34)
    return s


def slide11():
    s = Slide(11)
    s.box(160, 243, 784, 81)
    s.text(192, 294, "Прочитать OrderCreated", size=30)
    s.path([(552, 324), (552, 357)])
    s.box(160, 360, 784, 202, stroke=GREEN)
    s.text(192, 402, "Одна транзакция SQLite", color=GREEN, size=30)
    s.text(192, 446, "Списание 500 ₽", size=30)
    s.text(192, 490, "Обработанный event_id", size=30)
    s.text(192, 534, "Outbox · PaymentSucceeded", size=28, mono=True, color=BLUE)
    s.path([(552, 562), (552, 644)])
    s.cross(552, 604)
    s.label(340, 611, "Сбой после транзакции", color=RED, size=28)
    s.box(160, 648, 784, 75, dashed=True)
    s.text(192, 696, "Зафиксировать offset в Kafka", size=30, color=MUTED)
    s.text(192, 759, "До сбоя не выполнено", size=26, color=RED)
    s.stage = 1
    s.path([(552, 604), (552, 628), (111, 628), (111, 284), (160, 284)], BLUE)
    s.label(335, 216, "Перезапуск · повторное чтение", color=BLUE, size=25)
    for y, n, a, b in [(270, "1", "запись", "OrderCreated"),
                        (402, "2", "попытки", "обработки"), (534, "1", "списание", "500 ₽")]:
        s.text(1056, y+45, n, size=64, weight=600, color=BLUE if n=="2" else GREEN)
        s.text(1136, y+9, a, size=30)
        s.text(1136, y+51, b, size=28, mono=b=="OrderCreated")
    s.lines(1056, 655, ["Те же topic, partition,", "offset и event_id"], size=25, color=MUTED, gap=36)
    s.stage = 2
    s.box(180, 460, 690, 42, fill="none", stroke=BLUE, radius=6)
    s.text(1056, 747, "Повтор распознан", size=32, color=BLUE)
    s.text(96, 823, "Идемпотентность — повтор сохраняет результат операции", size=33)
    return s


def slide12():
    s = Slide(12)
    receipt(s, 96, 279)
    s.text(544, 242, "По истории выбранного заказа", size=24, color=MUTED)
    s.text(568, 294, "Известный факт", weight=600, size=28)
    s.text(982, 294, "Что сообщаем покупателю", weight=600, size=28)
    def row(y, event, message, color):
        s.box(544, y, 960, 88, stroke=CARD, radius=8)
        s.box(544, y, 5, 88, stroke=color, fill=color, radius=2)
        s.text(568, y+50, event, mono=True, size=28)
        s.lines(982, y+35, message, gap=34, size=28)
    row(320, "PaymentSucceeded", ["Оплата прошла.", "Ждём бронирование"], YELLOW)
    s.stage = 1
    row(420, "SeatReserved", ["Билет подтверждён"], GREEN)
    s.stage = 2
    row(520, "SeatReservationFailed", ["Мест нет.", "Возврат в процессе"], RED)
    row(620, "PaymentRefunded", ["Деньги возвращены"], GREEN)
    s.stage = 3
    s.lines(96, 786, ["Какой факт подтверждает", "завершение операции пользователя?"], gap=47, size=38, color=YELLOW)
    return s


HEADER = '''---
theme:
  override:
    default:
      margin:
        percent: 0
      colors:
        foreground: "F5F7FA"
        background: "10151D"
    footer:
      style: empty
---

'''


def build():
    converter = shutil.which("rsvg-convert")
    if not converter:
        raise SystemExit("rsvg-convert is required to rebuild PNG assets")
    ASSETS.mkdir(parents=True, exist_ok=True)
    font_dir = ASSETS / "fonts"
    for name in ("Inter.ttf", "IBMPlexMono-Regular.ttf"):
        if not (font_dir / name).is_file():
            raise SystemExit(f"missing bundled font: {name}")
    manifest, frames, finals = [], [], []
    functions = [slide1, slide2, slide3, slide4, slide5, slide6, slide7,
                 slide8, slide9, slide10, slide11, slide12]
    with tempfile.TemporaryDirectory(prefix="ticket-slide-fonts-") as temp:
        config = Path(temp) / "fonts.conf"
        config.write_text('<?xml version="1.0"?><!DOCTYPE fontconfig SYSTEM "urn:fontconfig:fonts.dtd">'
                          f'<fontconfig><dir>{escape(str(font_dir))}</dir><cachedir>{escape(temp)}/cache</cachedir>'
                          '</fontconfig>')
        # macOS Pango defaults to CoreText, which ignores private fontconfig dirs.
        env = dict(os.environ, FONTCONFIG_FILE=str(config), PANGOCAIRO_BACKEND="fc")
        for number, author in enumerate(functions, 1):
            slide = author()
            for frame in range(COUNTS[number-1]):
                stem = f"{number:02d}-{chr(65+frame)}"
                svg, png = ASSETS / f"{stem}.svg", ASSETS / f"{stem}.png"
                svg.write_text(slide.svg(frame), encoding="utf-8")
                subprocess.run([converter, str(svg), "-o", str(png)], env=env, check=True,
                               capture_output=True, timeout=30)
                note = f"Слайд {number} / 12 · кадр {chr(65+frame)} · {TIMES[number-1]}. См. slides/speaker-notes.md."
                part = f"<!-- speaker_note: {note} -->\n\n![image:width:100%](assets/{stem}.png)\n"
                frames.append(part)
                if frame == COUNTS[number-1]-1:
                    finals.append(part)
                manifest.append({"slide": number, "frame": chr(65+frame), "physical_slide": len(frames),
                                 "title": TITLES[number-1], "time": TIMES[number-1],
                                 "svg": f"assets/{stem}.svg", "png": f"assets/{stem}.png"})
    for name, parts in [("talk.md", frames), ("final.md", finals)]:
        (ROOT / "slides" / name).write_text(HEADER + "\n<!-- end_slide -->\n\n".join(parts), encoding="utf-8")
    (ASSETS / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2)+"\n", encoding="utf-8")
    print(json.dumps({"logical_slides": len(functions), "frames": len(frames), "final_frames": len(finals)}))


if __name__ == "__main__":
    build()
