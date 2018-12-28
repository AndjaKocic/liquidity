import os
import sys
import csv
from datetime import datetime, timedelta

from dateutil.parser import parse
from dateutil.relativedelta import relativedelta

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from db import engine, text, Session, Security, PrefferedStockOrderLog, OrdinaryStockOrderLog, BondOrderLog, TradeLog


seccode = 'SBER'
date = '2015-09-01'
timestamp0 = None
timestamp1 = None


def part1(session):
    order_log_paths = []
    trade_log_paths = []
    securities_path = 'ListingSecurityList.csv'

    for n in sorted(os.listdir('OrderLog')):
        p = os.path.join('OrderLog', n)

        if not os.path.isdir(p):
            continue

        ps = sorted(os.listdir(p))
        order_log_filename, trade_log_filename = ps
        order_log_path = os.path.join(p, order_log_filename)
        trade_log_path = os.path.join(p, trade_log_filename)
        order_log_paths.append(order_log_path)
        trade_log_paths.append(trade_log_path)

    print('insert securities')

    with open(securities_path) as csvfile:
        reader = csv.DictReader(csvfile)
        securities = []

        for row in reader:
            supertype = row['SUPERTYPE']
            instrument_type = row['INSTRUMENT_TYPE']
            seccode = row['TRADE_CODE']

            security = Security(
                supertype = supertype,
                instrument_type = instrument_type,
                seccode = seccode,
            )

            securities.append(security)

        session.add_all(securities)
        session.flush()


    print('insert order_log')

    # for p in order_log_paths:
    for p in order_log_paths[:2]:
        dt = p[-12:-4]
        dt = parse(dt)

        with open(p) as csvfile:
            reader = csv.DictReader(csvfile)
            order_logs = []

            for row in reader:
                seccode = row['SECCODE']

                q = session.query(Security)
                q = q.filter(Security.seccode==seccode)
                security = q.first()

                if not security:
                    continue

                if not security.instrument_type:
                    continue

                if security.supertype == 'Облигации':
                    order_log = BondOrderLog(
                        no = row['NO'],
                        seccode = row['SECCODE'],
                        buysell = row['BUYSELL'],
                        time = (dt + timedelta(milliseconds=int(row['TIME']) - 100000000)).timestamp(),
                        orderno = row['ORDERNO'],
                        action = row['ACTION'],
                        price = row['PRICE'],
                        volume = row['VOLUME'],
                        tradeno = row['TRADENO'],
                        tradeprice = row['TRADEPRICE'] or 0,
                    )
                elif security.supertype == 'Акции':
                    if security.instrument_type == 'Акция обыкновенная':
                        order_log = OrdinaryStockOrderLog(
                            no = row['NO'],
                            seccode = row['SECCODE'],
                            buysell = row['BUYSELL'],
                            time = (dt + timedelta(milliseconds=int(row['TIME']) - 100000000)).timestamp(),
                            orderno = row['ORDERNO'],
                            action = row['ACTION'],
                            price = row['PRICE'],
                            volume = row['VOLUME'],
                            tradeno = row['TRADENO'],
                            tradeprice = row['TRADEPRICE'] or 0,
                        )
                    elif security.instrument_type == 'Акция привилегированная':
                        order_log = PrefferedStockOrderLog(
                            no = row['NO'],
                            seccode = row['SECCODE'],
                            buysell = row['BUYSELL'],
                            time = (dt + timedelta(milliseconds=int(row['TIME']) - 100000000)).timestamp(),
                            orderno = row['ORDERNO'],
                            action = row['ACTION'],
                            price = row['PRICE'],
                            volume = row['VOLUME'],
                            tradeno = row['TRADENO'],
                            tradeprice = row['TRADEPRICE'] or 0,
                        )
                    else:
                        continue
                else:
                    continue

                order_logs.append(order_log)

                if len(order_logs) > 500_000:
                    break
            
            session.add_all(order_logs)
            session.flush()

    print('insert trade_log')

    # for p in trade_log_paths:
    for p in trade_log_paths[:2]:
        dt = p[-12:-4]
        dt = parse(dt)

        with open(p) as csvfile:
            reader = csv.DictReader(csvfile)
            trade_logs = []

            for row in reader:
                trade_log = TradeLog(
                    tradeno = row['TRADENO'],
                    seccode = row['SECCODE'],
                    time = (dt + timedelta(seconds=int(row['TIME']) - 100000)).timestamp(),
                    buyorderno = row['BUYORDERNO'],
                    sellorderno = row['SELLORDERNO'],
                    price = row['PRICE'],
                    volume = row['VOLUME'],
                )

                trade_logs.append(trade_log)

                if len(trade_logs) > 500_000:
                    break
            
            session.add_all(trade_logs)
            session.flush()


def part2(session):
    global seccode, timestamp

    sql_query = 'select count(*) from security'
    n_securities = list(engine.execute(text(sql_query)))
    print('n_securities:', n_securities)

    sql_query = 'select count(*) from preffered_stock_order_log'
    n_preffered_stock_order_logs = list(engine.execute(text(sql_query)))[0][0]
    print('n_preffered_stock_order_logs:', n_preffered_stock_order_logs)

    sql_query = 'select count(*) from ordinary_stock_order_log'
    n_ordinary_stock_order_logs = list(engine.execute(text(sql_query)))[0][0]
    print('n_ordinary_stock_order_logs:', n_ordinary_stock_order_logs)

    sql_query = 'select count(*) from bond_order_log'
    n_bond_order_logs = list(engine.execute(text(sql_query)))[0][0]
    print('n_bond_order_logs:', n_bond_order_logs)

    sql_query = 'select count(*) from trade_log'
    n_trade_logs = list(engine.execute(text(sql_query)))[0][0]
    print('n_trade_logs:', n_trade_logs)

    # 2.d
    print(f'enter seccode ({seccode}):')
    seccode = input() or seccode
    print('seccode:', seccode)

    print(f'enter date ({date}):')
    dt0 = input() or date
    dt0 = parse(dt0)
    timestamp0 = dt0.timestamp()
    print('timestamp0:', timestamp0)

    dt1 = dt0 + timedelta(days=1)
    timestamp1 = dt1.timestamp()
    print('timestamp1:', timestamp1)

    sql_query = f'select count(distinct seccode) from trade_log where time between {timestamp0} and {timestamp1}'
    total_n_uniquq_seccodes = list(engine.execute(text(sql_query)))[0][0]
    print('total_n_uniquq_seccodes:', total_n_uniquq_seccodes)

    sql_query = f'select sum(volume) from trade_log where time between {timestamp0} and {timestamp1}'
    total_volume_of_all_trades = list(engine.execute(text(sql_query)))[0][0]
    print('total_volume_of_all_trades:', total_volume_of_all_trades)

    sql_query = f'select count(*) from trade_log where seccode="{seccode}" and time between {timestamp0} and {timestamp1}'
    total_trades_for_seccode = list(engine.execute(text(sql_query)))[0][0]
    print('total_trades_for_seccode:', total_trades_for_seccode)

    sql_query = f'select sum(volume) from trade_log where seccode="{seccode}" and time between {timestamp0} and {timestamp1}'
    total_volume_for_seccode = list(engine.execute(text(sql_query)))[0][0]
    print('total_volume_for_seccode:', total_volume_for_seccode)

    sql_query = f'select min(volume), max(volume) from trade_log where seccode="{seccode}" and time between {timestamp0} and {timestamp1}'
    min_max_volume_for_seccode = list(engine.execute(text(sql_query)))[0]
    print('min_max_volume_for_seccode:', min_max_volume_for_seccode)
    avg_volume_for_seccode = min_max_volume_for_seccode[1] - min_max_volume_for_seccode[0]
    print('avg_volume_for_seccode:', avg_volume_for_seccode)

    sql_query = f'select min(price), max(price) from trade_log where seccode="{seccode}" and time between {timestamp0} and {timestamp1}'
    min_max_price_for_seccode = list(engine.execute(text(sql_query)))[0]
    print('min_max_price_for_seccode:', min_max_price_for_seccode)
    avg_price_for_seccode = min_max_price_for_seccode[1] - min_max_price_for_seccode[0]
    print('avg_price_for_seccode:', avg_price_for_seccode)

    with open('part2.csv', 'w') as csvfile:
        writer = csv.writer(csvfile)
        
        writer.writerow([
            'seccode',
            'timestamp0',
            'timestamp1',
            'total_n_uniquq_seccodes',
            'total_volume_of_all_trades',
            'total_trades_for_seccode',
            'total_volume_for_seccode',
            'min_volume_for_seccode',
            'max_volume_for_seccode',
            'avg_volume_for_seccode',
            'min_price_for_seccode',
            'max_price_for_seccode',
            'avg_price_for_seccode',
        ])
        
        writer.writerow([
            seccode,
            timestamp0,
            timestamp1,
            total_n_uniquq_seccodes,
            total_volume_of_all_trades,
            total_trades_for_seccode,
            total_volume_for_seccode,
            min_max_volume_for_seccode[0],
            min_max_volume_for_seccode[1],
            avg_volume_for_seccode,
            min_max_price_for_seccode[0],
            min_max_price_for_seccode[1],
            avg_price_for_seccode,
        ])


def part3(session):
    global seccode, timestamp, date

    sql_query = f'select min(time), max(time) from trade_log'
    min_max_time = list(engine.execute(text(sql_query)))[0]
    min_time, max_time = min_max_time
    min_datetime = datetime.fromtimestamp(min_time)
    max_datetime = datetime.fromtimestamp(max_time)
    print('min_datetime:', min_datetime)
    print('max_datetime:', max_datetime)
    
    date = min_datetime.isoformat()
    print(f'enter date ({date}):')
    dt0 = input() or date
    dt0 = parse(dt0)
    timestamp0 = dt0.timestamp()
    print('timestamp0:', timestamp0)

    dt1 = dt0 + timedelta(days=1)
    timestamp1 = dt1.timestamp()
    print('timestamp1:', timestamp1)

    if not (min_time <= timestamp0 <= max_time):
        print('Invalid date')
        sys.exit(1)

    sql_query = f'select distinct seccode from trade_log where time between {timestamp0} and {timestamp1}'
    seccodes = list(engine.execute(text(sql_query)))
    seccodes = [n[0] for n in seccodes]
    print('seccodes:', seccodes)

    print(f'enter seccode ({seccode}):')
    seccode = input() or seccode
    print('seccode:', seccode)

    buy_order_logs = []
    sql_query = f'select price, sum(volume) from preffered_stock_order_log where seccode="{seccode}" and buysell = "B" and time between {timestamp0} and {timestamp1} and price > 0 group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from ordinary_stock_order_log where seccode="{seccode}" and buysell = "B" and time between {timestamp0} and {timestamp1} and price > 0 group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from bond_order_log where seccode="{seccode}" and buysell = "B" and time between {timestamp0} and {timestamp1} and price > 0 group by price order by price desc'
    buy_order_logs += list(engine.execute(text(sql_query)))
    
    sell_order_logs = []
    sql_query = f'select price, sum(volume) from preffered_stock_order_log where seccode="{seccode}" and buysell = "S" and time between {timestamp0} and {timestamp1} and price > 0 group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from ordinary_stock_order_log where seccode="{seccode}" and buysell = "S" and time between {timestamp0} and {timestamp1} and price > 0 group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    sql_query = f'select price, sum(volume) from bond_order_log where seccode="{seccode}" and buysell = "S" and time between {timestamp0} and {timestamp1} and price > 0 group by price order by price asc'
    sell_order_logs += list(engine.execute(text(sql_query)))
    
    best_5_buy_order_logs = buy_order_logs[:5]
    best_5_sell_order_logs = sell_order_logs[:5]
    min_buy = max(best_5_buy_order_logs)[0]
    min_sell = min(best_5_sell_order_logs)[0]
    spread = min_buy - min_sell
    avg_price = (min_sell + min_buy) / 2.0
    print('spread:', spread, '(', min_sell, '-', min_buy, ')')
    print('avg_price:', avg_price)
    print('best_5_buy_order_logs:', best_5_buy_order_logs)
    print('best_5_sell_order_logs:', best_5_sell_order_logs)
    print('sum of best_5_buy_order_logs:', sum(n[0] for n in best_5_buy_order_logs))
    print('sum of best_5_sell_order_logs:', sum(n[0] for n in best_5_sell_order_logs))

    fig, ax = plt.subplots()
    ax.plot([n[0] for n in best_5_buy_order_logs], [n[1] for n in best_5_buy_order_logs], 'k--', label='Bid')
    ax.plot([n[0] for n in best_5_sell_order_logs], [n[1] for n in best_5_sell_order_logs], 'k:', label='Ask')
    ax.plot(
        [n[0] for n in (best_5_buy_order_logs + best_5_sell_order_logs)],
        [avg_price for n in (best_5_buy_order_logs + best_5_sell_order_logs)],
        'k',
        label='MID',
    )
    legend = ax.legend(loc='upper center', shadow=True, fontsize='x-large')
    path = 'part3.png'
    fig.suptitle('Отчет', fontsize=10)
    plt.savefig(path)

def part4(session):
    pass


def main():
    session = Session()

    if not os.path.exists('liquidity-analysis.db'):
        part1(session)
    
    part2(session)
    part3(session)
    part4(session)
    
    session.commit()
    session.close()


if __name__ == '__main__':
    main()
