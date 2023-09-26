class TradeConfiguration:
    def __init__(self):
        self.buy_risk = 0.02  #0.0213
        self.sell_risk = 0.02  #0.0123
        
        
    def get_risk(self,over_all_trend,current_signal):
        if over_all_trend  == "Uptrend":
            if current_signal == 'Buy':  
                risk = self.buy_risk
            else:
                risk = self.sell_risk/2
        else:
            if current_signal == 'Buy':
                risk = self.buy_risk/1.5
            else:
                risk = self.sell_risk
            
        ##notifier(f'Overall Trend is {over_all_trend} and Current Trend is {current_signal}, so risk is set to {risk}')
        return risk   
    
class PivotSuperTrendConfiguration():
    def __init__(self,period = 1, atr_multiplier = 1, pivot_period = 1):
        self.period = period
        self.atr_multiplier = atr_multiplier
        self.pivot_period = pivot_period
        
class TradeHistory():
    def __init__(self):
        self.order_ids = []

    def add_order_id(self,order_id):
        self.order_ids.append(order_id)
        
    def remove_order_id(self,order_id):
        self.order_ids.remove(order_id)

    def get_order_ids(self):
        return self.order_ids
        
class Order:
    def __init__(self,coin,entry,quantity,round_price,take_profit = None , stop_loss = None,change = None ,
                  partial_profit_take = None, lowerband = None , upperband = None):
        self.coin = coin.upper()
        self.quantity = quantity
        self.take_profit = take_profit
        self.stop_loss = stop_loss
        self.entry = entry
        self.round_price = round_price
        self.change = change
        self.partial_profit_take = partial_profit_take
        self.lowerband = lowerband
        self.upperband = upperband
        
    def make_buy_trade(self,client):
        
        client.futures_create_order(symbol=f'{self.coin}USDT', side='BUY', type='MARKET', quantity=self.quantity, dualSidePosition=True, positionSide='LONG')
             
        if self.change == 'longTerm':
            #notifier(f'Pivot SuperTrend Changed')
            self.take_profit = self.entry+((self.entry*0.0213))
        else:
            self.take_profit = self.entry+((self.entry*0.06))

        #notifier(f'Placing tp order at {round(self.take_profit, self.round_price)}')
        
        client.futures_create_order(
                                    symbol=f'{self.coin}USDT',
                                    price=round(self.take_profit, self.round_price),
                                    side='SELL',
                                    positionSide='LONG',
                                    quantity=self.partial_profit_take,
                                    timeInForce='GTC',
                                    type='LIMIT',
                                    # reduceOnly=True,cc
                                    closePosition=False,
                                    # stopPrice=round(take_profit,2),
                                    workingType='MARK_PRICE',
                                    priceProtect=True
                                )
        #notifier(f'Coin :{self.coin}, Quantity : {self.quantity } stake : {round(self.quantity*self.entry,2)}')
        #notifier(f'Buy order placed for coin :{self.coin}, TP : {self.take_profit}')
        
    def make_sell_trade(self,client):
        
        
        client.futures_create_order(
                                        symbol=f'{self.coin}USDT', side='SELL', 
                                        type='MARKET',
                                        quantity=self.quantity,
                                        dualSidePosition=True, 
                                        positionSide='SHORT'
                                    )
        
        if self.change == 'longTerm':
            #notifier(f'Pivot SuperTrend Changed')
            self.take_profit = self.entry-((self.entry*0.0213))
        else:
            self.take_profit = self.entry-((self.entry*0.0411))

        #notifier(f'Placing tp order at {round(self.take_profit, self.round_price)}')
        
        client.futures_create_order(
                                    symbol=f'{self.coin}USDT',
                                    price=round(self.take_profit, self.round_price),
                                    side='BUY',
                                    positionSide='SHORT',
                                    quantity=self.partial_profit_take,
                                    timeInForce='GTC',
                                    type='LIMIT',
                                    # reduceOnly=True,
                                    closePosition=False,
                                    # stopPrice=round(take_profit,2),
                                    workingType='MARK_PRICE',
                                    priceProtect=True
                               )
        
        #notifier(f'Coin :{self.coin}, Quantity : {self.quantity } stake : {round(self.quantity*self.entry,2)}')
        #notifier(f'Sell order placed for coin :{self.coin}, TP : {self.take_profit}')
        
    def make_inverse_buy_trade(self,client):
        client.futures_create_order(symbol=f'{self.coin}USDT', side='BUY', type='MARKET', quantity=self.quantity, dualSidePosition=True, positionSide='LONG')

        difference = self.upperband - self.entry

        self.take_profit = self.upperband 

        stop_loss = self.entry - (1.6 * difference)

        #notifier(f'take profit : {self.take_profit},round : {self.round_price} ,after round : {round(self.take_profit, self.round_price)},difference : {difference} , 2nd entry : {stop_loss}')

        client.futures_create_order(
                                    symbol=f'{self.coin}USDT',
                                    price=round(self.take_profit, self.round_price),
                                    side='SELL',
                                    positionSide='LONG',
                                    quantity=self.quantity,
                                    timeInForce='GTC',
                                    type='LIMIT',
                                    # reduceOnly=True,cc
                                    closePosition=False,
                                    # stopPrice=round(take_profit,2),
                                    workingType='MARK_PRICE',
                                    priceProtect=True
                                )
        
        #notifier(f'Placed Take Profit order for long position')

        client.futures_create_order(
                            symbol=f'{self.coin}USDT',
                            side='BUY',
                            positionSide='LONG',
                            price=round(stop_loss, self.round_price),  # Using stop_loss as the limit price for buying
                            quantity=self.quantity,
                            timeInForce='GTC',
                            type='LIMIT',
                            workingType='MARK_PRICE'
                            )
        
        #notifier(f'Placed Buy Limit order for long position at {round(stop_loss, self.round_price)}')



        #notifier(f'Coin :{self.coin}, Quantity : {self.quantity } stake : {round(self.quantity*self.entry,2)}')
        #notifier(f'Buy order placed for coin :{self.coin}, TP : {self.take_profit}')

    def make_inverse_sell_trade(self,client):
        client.futures_create_order(
                                        symbol=f'{self.coin}USDT', side='SELL', 
                                        type='MARKET',
                                        quantity=self.quantity,
                                        dualSidePosition=True, 
                                        positionSide='SHORT'
                                    )
        
        difference = self.entry - self.lowerband 

        self.take_profit = self.lowerband 

        stop_loss = self.entry + (1.6 * difference)

        #notifier(f'take profit : {self.take_profit}, difference : {difference} , 2nd entry : {stop_loss}')
        
        client.futures_create_order(
                                    symbol=f'{self.coin}USDT',
                                    price=round(self.take_profit, self.round_price),
                                    side='BUY',
                                    positionSide='SHORT',
                                    quantity=self.quantity,
                                    timeInForce='GTC',
                                    type='LIMIT',
                                    # reduceOnly=True,
                                    closePosition=False,
                                    # stopPrice=round(take_profit,2),
                                    workingType='MARK_PRICE',
                                    priceProtect=True
                               )
        
        #notifier(f'Placed Take Profit order for short position')

        # client.futures_create_order(
        #                             symbol=f'{self.coin}USDT',
        #                             side='BUY',                   # Change to 'BUY' because you're covering a short position
        #                             positionSide='SHORT',        # Indicate that the position is a 'SHORT'
        #                             quantity=self.quantity,
        #                             type='STOP_MARKET',
        #                             stopPrice=round(stop_loss, self.round_price),
        #                             closePosition=True,
        #                             workingType='MARK_PRICE'
        #                         )
        

        client.futures_create_order(
                            symbol=f'{self.coin}USDT',
                            side='SELL',
                            positionSide='LONG',
                            price=round(stop_loss, self.round_price),  # Using sell_price as the limit price for selling
                            quantity=self.quantity,
                            timeInForce='GTC',
                            type='LIMIT',
                            workingType='MARK_PRICE'
                            )
        
        #notifier(f'Placed Take Stoploss market order for short position')
        #notifier(f'Coin :{self.coin}, Quantity : {self.quantity } stake : {round(self.quantity*self.entry,2)}')
        #notifier(f'Sell order placed for coin :{self.coin}, TP : {self.take_profit}')

class CurrentTrade:
    def __init__(self,coin,stake,timeframe,use_sl,round_quantity = None,round_price = None,check_for_volatilte_coin=0):
        self.coin = coin
        self.stake = stake
        self.timeframe = timeframe
        self.round_quantity = round_quantity
        self.round_price = round_price 
        self.check_for_volatilte_coin = check_for_volatilte_coin
        self.use_sl = use_sl

    def get_current_coin(self):
        return self.coin
    
    def set_current_coin(self,coin):
        self.coin = coin