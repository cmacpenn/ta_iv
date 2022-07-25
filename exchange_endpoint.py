from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only
from datetime import datetime
import sys

from models import Base, Order, Log
engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)

@app.before_request
def create_session():
    g.session = scoped_session(DBSession)

@app.teardown_appcontext
def shutdown_session(response_or_exc):
    sys.stdout.flush()
    g.session.commit()
    g.session.remove()


""" Suggested helper methods """

def check_sig(payload,sig):
    #pass
    # extract order information
    sender_pk = payload['sender_pk']
    platform = payload['platform']  # crypto platform

    payload = json.dumps('payload') # dump the payload message

    # =====code from Exchange Server I=====
    # verify sig for ETH
    if platform == 'Ethereum':
        eth_encoded_msg = eth_account.messages.encode_defunct(text=payload)
        if eth_account.Account.recover_message(eth_encoded_msg,signature=sig) == sender_pk:
            result = True
        else:
            result = False

    # verify sig for Algorand
    elif platform == 'Algorand':
        if algosdk.util.verify_bytes(payload.encode('utf-8'), sig, sender_pk):
            result = True
        else:
            result = False
    # other crypto platform
    else:
        result = False

    return result

def fill_order(order,txes=[]):
    #pass
    # 1.Insert order into database
    # sender_pk = order['sender_pk']
    # receiver_pk = order['receiver_pk']
    # buy_currency = order['buy_currency']
    # sell_currency = order['sell_currency']
    # buy_amount = order['buy_amount']
    # sell_amount = order['sell_amount']

    sender_pk = order.sender_pk
    receiver_pk = order.receiver_pk
    buy_currency = order.buy_currency
    sell_currency = order.sell_currency
    buy_amount = order.buy_amount
    sell_amount = order.sell_amount

    current_order = Order(sender_pk=sender_pk, receiver_pk=receiver_pk, buy_currency=buy_currency,
                          sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount)
    g.session.add(current_order)
    g.session.commit()

    # 2.Check if there are any existing orders that match the new order
    existing_order = g.session.query(Order). \
        filter(Order.filled == None, Order.buy_currency == current_order.sell_currency,
               Order.sell_currency == current_order.buy_currency,
               ((Order.sell_amount * current_order.sell_amount) >= (Order.buy_amount * current_order.buy_amount))) \
        .first()  # FIFO

    # 3. Fill the matched order
    if existing_order != None:

        # Set the filled field to be the current timestamp on both orders
        current_order.filled = datetime.now()
        existing_order.filled = datetime.now()
        # Set counterparty_id to be the id of the other order
        current_order.counterparty_id = existing_order.id
        existing_order.counterparty_id = current_order.id

        # If one of the orders is not completely filled (i.e. the counterparty’s sell_amount is less than buy_amount):
        if existing_order.sell_amount < current_order.buy_amount or current_order.buy_amount < existing_order.sell_amount:

            if existing_order.sell_amount < current_order.buy_amount:
                sender_pk = current_order.sender_pk
                receiver_pk = current_order.receiver_pk
                buy_currency = current_order.buy_currency
                sell_currency = current_order.sell_currency

                buy_amount = current_order.buy_amount - existing_order.sell_amount
                sell_amount = buy_amount / (current_order.buy_amount / current_order.sell_amount)

                creator_id = current_order.id

            elif existing_order.sell_amount > current_order.buy_amount:
                sender_pk = existing_order.sender_pk
                receiver_pk = existing_order.receiver_pk
                buy_currency = existing_order.buy_currency
                sell_currency = existing_order.sell_currency

                sell_amount = existing_order.sell_amount - current_order.buy_amount
                buy_amount = sell_amount / (existing_order.sell_amount / existing_order.buy_amount)

                creator_id = existing_order.id

            # Create a new order for remaining balance
            # new_order = Order(sender_pk=sender_pk, receiver_pk=receiver_pk, buy_currency=buy_currency,
            #                   sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount, creator_id = creator_id)
            new_order = Order(sender_pk=sender_pk, receiver_pk=receiver_pk, buy_currency=buy_currency,
                              sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount)

            new_order.creator_id = creator_id

            g.session.add(new_order)
            g.session.commit()
            #process_order(new_order)

        else:
            g.session.commit()
  
def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    # Hint: use json.dumps or str() to get it in a nice string form
    m = json.dumps(d)
    log_m = Log(message = m)
    g.session.add(log_m)
    g.session.commit()
    #pass

""" End of helper methods """



@app.route('/trade', methods=['POST'])
def trade():
    print("In trade endpoint")
    if request.method == "POST":
        content = request.get_json(silent=True)
        print( f"content = {json.dumps(content)}" )
        columns = [ "sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform" ]
        fields = [ "sig", "payload" ]

        for field in fields:
            if not field in content.keys():
                print( f"{field} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
        
        for column in columns:
            if not column in content['payload'].keys():
                print( f"{column} not received by Trade" )
                print( json.dumps(content) )
                log_message(content)
                return jsonify( False )
            
        #Your code here
        #Note that you can access the database session using g.session

        # TODO: Check the signature
        result = check_sig(content['payload'], content['sig'])

        # log the message before return the False
        if result == False:
            log_message(content)
            return jsonify(False)

        # TODO: Add the order to the database

        payload = content['payload']
        sender_pk = payload['sender_pk']
        receiver_pk = payload['receiver_pk']
        buy_currency = payload['buy_currency']
        sell_currency = payload['sell_currency']
        buy_amount = payload['buy_amount']
        sell_amount = payload['sell_amount']

        current_order = Order(sender_pk=sender_pk, receiver_pk=receiver_pk, buy_currency=buy_currency,
                           sell_currency=sell_currency, buy_amount=buy_amount, sell_amount=sell_amount)

        # TODO: Fill the order
        fill_order(current_order)
        
        # TODO: Be sure to return jsonify(True) or jsonify(False) depending on if the method was successful
        #return jsonify(result)
        if result == True:
            return jsonify(True)

@app.route('/order_book')
def order_book():
    #Your code here
    #Note that you can access the database session using g.session
    result = {'data': []}
    for this in g.session.query(Order).all():
        result['data'].append({'sender_pk': this.sender_pk,
                               'receiver_pk': this.receiver_pk,
                               'buy_currency': this.buy_currency,
                               'sell_currency': this.sell_currency,
                               'buy_amount': this.buy_amount,
                               'sell_amount': this.sell_amount,
                               'signature': this.signature})
    return jsonify(result)
    #return result

if __name__ == '__main__':
    app.run(port='5002')
