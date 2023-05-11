import aiosqlite
import asyncio
import os

from pathlib import Path


class UserDB:
    def __init__(self):
        self.work_dir = os.path.dirname(os.path.realpath(__file__))
        self.connector = aiosqlite.connect(Path(self.work_dir, 'users.db'))

    async def create_tables(self):
        async with self.connector as conn:
            sql = """CREATE TABLE IF NOT EXISTS users(
                        user_id INTEGER PRIMARY KEY,
                        username TEXT,
                        first_name TEXT,
                        last_name TEXT,
                        language_code TEXT,
                        date_add DATETIME
                        );
                    CREATE TABLE IF NOT EXISTS admins(
                        user_id INTEGER UNIQUE,
                        date_add DATETIME,
                        FOREIGN KEY (user_id) REFERENCES users(user_id) 
                        );
                    CREATE TABLE IF NOT EXISTS inactive_users(
                        user_id INTEGER UNIQUE,
                        inactive_flag INTEGER,
                        date_add DATETIME,
                        FOREIGN KEY (user_id) REFERENCES users(user_id) 
                        );
                    CREATE TABLE IF NOT EXISTS users_subscribe(
                        user_id INTEGER UNIQUE,
                        subscribe_flag INTEGER,
                        date_add DATETIME,
                        FOREIGN KEY (user_id) REFERENCES users(user_id) 
                        );
                    CREATE TABLE IF NOT EXISTS daily_reports(
                        report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        report_msg TEXT,
                        date_add DATETIME
                        );
                    CREATE TABLE IF NOT EXISTS weekly_reports(
                        report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        report_msg TEXT,
                        date_add DATETIME
                        );
                    CREATE TABLE IF NOT EXISTS monthly_reports(
                        report_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        report_msg TEXT,
                        date_add DATETIME
                        );
                    CREATE TABLE IF NOT EXISTS send_daily_report(
                        user_id INTEGER,
                        report_id INTEGER,
                        send_flag INTEGER,
                        date_add DATETIME,
                        UNIQUE(user_id, report_id),
                        FOREIGN KEY (user_id) REFERENCES users(user_id),
                        FOREIGN KEY (report_id) REFERENCES daily_reports(report_id)
                        );
                    CREATE TABLE IF NOT EXISTS send_weekly_report(
                        user_id INTEGER,
                        report_id INTEGER,
                        send_flag INTEGER,
                        date_add DATETIME,
                        UNIQUE(user_id, report_id),
                        FOREIGN KEY (user_id) REFERENCES users(user_id),
                        FOREIGN KEY (report_id) REFERENCES weekly_reports(report_id)
                        );
                    CREATE TABLE IF NOT EXISTS send_monthly_report(
                        user_id INTEGER,
                        report_id INTEGER,
                        send_flag INTEGER,
                        date_add DATETIME,
                        UNIQUE(user_id, report_id),
                        FOREIGN KEY (user_id) REFERENCES users(user_id),
                        FOREIGN KEY (report_id) REFERENCES monthly_reports(report_id) 
                        );
                    CREATE TABLE IF NOT EXISTS user_actions(
                        user_id INTEGER,
                        action TEXT,
                        date_add DATETIME,
                        FOREIGN KEY (user_id) REFERENCES users(user_id) 
                        );
                    CREATE TABLE IF NOT EXISTS owners(
                        owner_addr TEXT PRIMARY KEY,
                        owner_name TEXT,
                        date_add DATETIME
                        );
                    CREATE TABLE IF NOT EXISTS addrs_monitor(
                        user_id INTEGER,
                        addr_monitor TEXT,
                        monitor_flag INTEGER,
                        date_add TEXT,
                        UNIQUE (user_id, addr_monitor, monitor_flag),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                        ON DELETE CASCADE,
                        FOREIGN KEY (addr_monitor) REFERENCES owners(owner_addr)
                        ON DELETE CASCADE
                        );
                    CREATE TABLE IF NOT EXISTS sales_monitoring(
                        monitor_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        addr_monitor TEXT,
                        coll_addr TEXT,
                        coll_name TEXT,
                        token_name TEXT,
                        token_num INTEGER,
                        buyer_addr TEXT,
                        buyer_name TEXT,
                        price_stars INTEGER,
                        price_usd REAL,
                        date_create TEXT,
                        date_add TEXT,
                        UNIQUE (coll_addr, token_num, date_create),
                        FOREIGN KEY (addr_monitor) REFERENCES owners(owner_addr)
                        ON DELETE CASCADE
                        );
                    CREATE TABLE IF NOT EXISTS buys_monitoring(
                        monitor_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        addr_monitor TEXT,
                        coll_addr TEXT,
                        coll_name TEXT,
                        token_name TEXT,
                        token_num INTEGER,
                        seller_addr TEXT,
                        seller_name TEXT,
                        price_stars INTEGER,
                        price_usd REAL,
                        date_create TEXT,
                        date_add TEXT,
                        UNIQUE (coll_addr, token_num, date_create),
                        FOREIGN KEY (addr_monitor) REFERENCES owners(owner_addr)
                        ON DELETE CASCADE
                        );
                    CREATE TABLE IF NOT EXISTS send_monitor_info(
                        user_id INTEGER,
                        monitor_id INTEGER,
                        action TEXT,
                        send_flag INTEGER,
                        date_add TEXT,
                        UNIQUE (user_id, monitor_id, action),
                        FOREIGN KEY (user_id) REFERENCES users(user_id)
                        ON DELETE CASCADE,
                        FOREIGN KEY (monitor_id) REFERENCES sales_monitoring(monitor_id)
                        ON DELETE CASCADE
                        );"""
            await conn.executescript(sql)
            await conn.commit()


if __name__ == "__main__":
    asyncio.run(UserDB().create_tables())
