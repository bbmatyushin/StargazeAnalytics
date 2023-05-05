
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

### Запускаем Бота отдельным сервисным процессом
Находясь в директории с ботом, выполнить команды:
```shell
# Создаём виртуальное окружения для необходимых библиотек
python3 -m venv venv
```
```shell
# Устанавливаем зависимости
source venv/bin/activate && pip install -r requirements.txt
```
```shell
# Создаем сервисный файл запуска бота
printf "[Unit]
Description=Stargaze Analytics BOT
After=network-online.target

[Service]
User=$USER
WorkingDirectory=`pwd`
ExecStart=`pwd`/venv/bin/python3 bot_start.py
Restart=always
RestartSec=7

[Install]
WantedBy=multi-user.target" > /etc/systemd/system/stargaze_analytics_bot.service
```
```shell
systemctl daemon-reload
systemctl enable stargaze_analytics_bot.service
systemctl restart stargaze_analytics_bot.service
```