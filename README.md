
### Запускаем парсер через Docker
Собираем образ для запуска контейнера: 
```shell
sudo docker build -t sg_data_parser .
```

Запускаем контейнер (находясь внутри папки с ботом), с сохранением данных в БД на хосте:
```shell
sudo docker run \
--name=sg_analytics_parser \
-v `pwd`/database/stargaze_analytics.db:/StargazeParser/database/stargaze_analytics.db \
--restart=always \
-d \
sg_data_parser
```

Парсер запуститься через 120 секунд. Проверить логи можно командой:
```shell
docker logs -f sg_analytics_parser
```
Выходим из логов командой `Ctrl + C`

### Запускаем парсер отдельным сервисным процессом
```shell
printf "[Unit]
Description=Stargaze Parser Analytics
After=network-online.target

[Service]
User=$USER
WorkingDirectory=`pwd`
ExecStart=`pwd`/venv/bin/python3 parser_start.py
Restart=always
RestartSec=7

[Install]
WantedBy=multi-user.target" > /etc/systemd/system/stargaze_analytics_parser.service
```
```shell
systemctl daemon-reload
systemctl enable stargaze_analytics_parser.service
systemctl restart stargaze_analytics_parser.service
```