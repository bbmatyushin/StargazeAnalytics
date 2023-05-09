"""Сообщения для хендлера с командами"""

LEXICON_CMD_RU: dict = {
    "wallet_monitor_1": "Можно отслеживанить продажи сразу по 5 кошелькам.\n"
                        "Для наблюдения Вам доступно: ",
    "wallet_monitor_2": "Пришлите адрес кошелька:",
    "add_wallet_addr": "✅ Адрес успешно добавлен!\n\n"
                       "Вы получите сообщение, когда пройдёт новая продажа.",
    "dbl_wallet_addr": "🚫 Этот кошелёк уже отслеживается.\n\n"
                       "Чтобы добавить новый адрес, нажмите /wallet_monitor",
    "err_wallet_addr": "❌ <b>Не верный адрес</b>.\n\n"
                       "Адрес должен состоять из 44 символов.\n"
                       "Чтобы добавить новый адрес, нажмите /wallet_monitor"
}