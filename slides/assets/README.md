# Ресурсы презентации

`01-A.svg` … `12-D.svg` — редактируемые векторные кадры; одноимённые PNG — ресурсы для presenterm. Каждый кадр 1600×900, фон и текст непрозрачные, номер обозначает смысловой слайд из 12. Список 33 кадров, их порядок и интервалы выступления: `manifest.json`.

Авторская разметка находится в `scripts/build_slides.py`. При правке менять её и пересобирать из корня:

```bash
python3 scripts/build_slides.py
python3 scripts/check_slides.py
```

Генератор использует Python 3 и `rsvg-convert`, не обращается к сети. Создаёт временную изолированную конфигурацию fontconfig только с приложенными шрифтами и локальным временным кешем. `PANGOCAIRO_BACKEND=fc` важен на macOS: CoreText не использует приватный каталог fontconfig и подменяет шрифты. Готовые PNG независимы от установленных шрифтов; SVG при просмотре другим приложением может требовать загрузки приложенных гарнитур.

Шрифты получены из Google Fonts 2026-11-17 и включены с лицензиями SIL Open Font License:

- `fonts/Inter.ttf` — [Inter, variable opsz/wght](https://github.com/google/fonts/blob/main/ofl/inter/Inter%5Bopsz,wght%5D.ttf); лицензия `fonts/Inter-OFL.txt`.
- `fonts/IBMPlexMono-Regular.ttf` — [IBM Plex Mono Regular](https://github.com/google/fonts/blob/main/ofl/ibmplexmono/IBMPlexMono-Regular.ttf); лицензия `fonts/IBMPlexMono-OFL.txt`.

В кадрах используются Inter 400/600 и IBM Plex Mono 400. Цвета и тексты определены `docs/PRESENTATION_DESCRIPTION.md`. Финальные кадры для статического просмотра собраны в `slides/final.md`; полный порядок с раскрытиями — в `slides/talk.md`.
