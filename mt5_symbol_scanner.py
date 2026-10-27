# mt5_connection.py - COMPLETE UPDATED VERSION

import MetaTrader5 as mt5
import pandas as pd
import logging
import time
import asyncio
from datetime import datetime, timedelta

class MT5Connection:
    _instance = None
    _connected = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    async def initialize(self, account=None, password=None, server=None):
        """Initialize MT5 connection with ENHANCED error handling"""
        try:
            print(f"🔌 Attempting MT5 connection to {server}...")
            
            # First shutdown any existing connection
            try:
                mt5.shutdown()
            except:
                pass
            
            # Initialize MT5
            if not mt5.initialize():
                error = mt5.last_error()
                print(f"❌ MT5 initialization failed. Error: {error}")
                return False
            
            print("✅ MT5 initialized successfully")
            
            # Login if credentials provided
            if account and password and server:
                print(f"🔐 Attempting login - Account: {account}, Server: {server}")
                
                login_success = mt5.login(login=int(account), password=password, server=server)
                
                if not login_success:
                    error = mt5.last_error()
                    print(f"❌ MT5 login failed. Error: {error}")
                    return False
                
                print("✅ Login successful")
                
                # Verify account info
                account_info = mt5.account_info()
                if account_info:
                    print(f"💰 Connected to: {account_info.login}, Balance: {account_info.balance}")
                else:
                    print("⚠️  Could not get account info")
            
            self._connected = True
            return True
            
        except Exception as e:
            print(f"❌ Exception during MT5 initialization: {str(e)}")
            return False
    
    async def shutdown(self):
        """Shutdown MT5 connection"""
        try:
            mt5.shutdown()
            self._connected = False
            logging.info("MT5 connection closed")
        except Exception as e:
            logging.error(f"Error shutting down MT5: {str(e)}")
    
    def is_connected(self):
        return self._connected
    
    async def get_account_info(self):
        """Get account information"""
        try:
            account_info = mt5.account_info()
            if account_info is None:
                logging.error("Failed to get account info")
                return None
            return account_info
        except Exception as e:
            logging.error(f"Error getting account info: {str(e)}")
            return None
    
    async def get_balance(self):
        """Get account balance"""
        try:
            account_info = await self.get_account_info()
            if account_info:
                return {
                    'balance': account_info.balance,
                    'equity': account_info.equity,
                    'margin': account_info.margin,
                    'free_margin': account_info.margin_free,
                    'currency': account_info.currency
                }
            return {'balance': 0, 'equity': 0, 'margin': 0, 'free_margin': 0, 'currency': 'USD'}
        except Exception as e:
            logging.error(f"Error getting balance: {str(e)}")
            return {'balance': 0, 'equity': 0, 'margin': 0, 'free_margin': 0, 'currency': 'USD'}
    
    async def get_symbol_info(self, symbol):
        """Get symbol information"""
        try:
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                logging.warning(f"Symbol {symbol} not found in MT5")
                return None
            return symbol_info
        except Exception as e:
            logging.error(f"Error getting symbol info for {symbol}: {str(e)}")
            return None
    
    async def get_current_price(self, symbol):
        """Get current price for a symbol"""
        try:
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                logging.error(f"Failed to get tick data for {symbol}")
                return None
            return (tick.bid + tick.ask) / 2  # Return mid price
        except Exception as e:
            logging.error(f"Error getting price for {symbol}: {str(e)}")
            return None
    
    async def get_ohlcv(self, symbol, timeframe='M15', count=100):
        """Get OHLCV data for a symbol"""
        try:
            # Map timeframe string to MT5 constant
            tf_mapping = {
                'M1': mt5.TIMEFRAME_M1,
                'M5': mt5.TIMEFRAME_M5,
                'M15': mt5.TIMEFRAME_M15,
                'M30': mt5.TIMEFRAME_M30,
                'H1': mt5.TIMEFRAME_H1,
                'H4': mt5.TIMEFRAME_H4,
                'D1': mt5.TIMEFRAME_D1
            }
            
            timeframe_val = tf_mapping.get(timeframe, mt5.TIMEFRAME_M15)
            rates = mt5.copy_rates_from_pos(symbol, timeframe_val, 0, count)
            
            if rates is None:
                logging.warning(f"No data returned for {symbol} {timeframe}")
                return None
            
            # Convert to DataFrame
            df = pd.DataFrame(rates)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df.set_index('time', inplace=True)
            
            return df
        except Exception as e:
            logging.error(f"Error getting OHLCV for {symbol}: {str(e)}")
            return None
    
    async def place_order(self, symbol, order_type, volume, price=None, 
                         stop_loss=None, take_profit=None, comment="", magic=12345):
        """Place an order in MT5 with PROPER retcode and comment handling"""
        try:
            print(f"🔧 Starting order for {symbol}")
            
            # VERIFY CONNECTION FIRST
            if not self.is_connected():
                print("🔄 MT5 not connected, attempting reconnect...")
                from dotenv import load_dotenv
                import os
                load_dotenv()
                
                mt5_account = os.getenv("MT5_ACCOUNT")
                mt5_password = os.getenv("MT5_PASSWORD") 
                mt5_server = os.getenv("MT5_SERVER")
                
                if mt5_account and mt5_password and mt5_server:
                    success = await self.initialize(int(mt5_account), mt5_password, mt5_server)
                    if not success:
                        print("❌ Reconnection failed")
                        return None
            
            # Check if trading is allowed
            terminal_info = mt5.terminal_info()
            if terminal_info and not terminal_info.trade_allowed:
                print("❌ TRADING DISABLED - Enable AutoTrading (Ctrl+E)")
                return None
            
            # Ensure symbol is selected and visible
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                print(f"❌ Symbol {symbol} not found")
                return None
            
            if not symbol_info.visible:
                print(f"🔄 Selecting symbol {symbol}...")
                selected = mt5.symbol_select(symbol, True)
                if not selected:
                    print(f"❌ Failed to select symbol {symbol}")
                    return None
            
            # Get current tick data
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                print(f"❌ No tick data for {symbol}")
                return None
            
            print(f"📊 {symbol} - Bid: {tick.bid}, Ask: {tick.ask}")
            
            # FIX: SHORTEN COMMENT to avoid MT5 limits (max ~25-30 chars)
            short_comment = comment[:25] if comment else "Forex DCA"
            print(f"💬 Comment: '{short_comment}'")
            
            # Prepare order request
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": float(volume),
                "deviation": 20,
                "magic": magic,
                "comment": short_comment,  # USE SHORTENED COMMENT
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            # Set order type and price
            if order_type == mt5.ORDER_TYPE_BUY:
                request["type"] = mt5.ORDER_TYPE_BUY
                request["price"] = tick.ask  # Use ASK for BUY
                print(f"🟢 BUY Order: {volume} lots at {tick.ask}")
            else:  # ORDER_TYPE_SELL
                request["type"] = mt5.ORDER_TYPE_SELL  
                request["price"] = tick.bid  # Use BID for SELL
                print(f"🔴 SELL Order: {volume} lots at {tick.bid}")
            
            # Add stop loss and take profit if provided
            if stop_loss:
                request["sl"] = float(stop_loss)
                print(f"🛑 Stop Loss: {stop_loss}")
            if take_profit:
                request["tp"] = float(take_profit)
                print(f"🎯 Take Profit: {take_profit}")
            
            # Send order
            print(f"📤 Sending order...")
            result = mt5.order_send(request)
            
            if result is None:
                error = mt5.last_error()
                print(f"❌ Order send returned None. Error: {error}")
                return None
                
            print(f"📨 Order Result:")
            print(f"   - Retcode: {result.retcode}")
            print(f"   - Comment: {result.comment}")
            print(f"   - Order: {result.order}")
            print(f"   - Volume: {result.volume}")
            
            # CORRECT RETCODE HANDLING - 10009 means SUCCESS!
            success_codes = [mt5.TRADE_RETCODE_DONE, 10009, 10008]  # All success codes
            
            if result.retcode not in success_codes:
                print(f"❌ Order failed: {result.retcode} - {result.comment}")
                
                # Error guidance
                error_guidance = {
                    10004: "Requote - prices changed",
                    10006: "Request rejected",
                    10007: "Request canceled by trader", 
                    10010: "Partial execution",
                    10011: "Request processing error",
                    10012: "Request canceled by timeout",
                    10013: "Invalid request",
                    10014: "Invalid volume",
                    10015: "Invalid price",
                    10016: "Invalid stops",
                    10017: "Trade disabled",
                    10018: "Market closed",
                    10019: "Not enough funds",
                    10020: "Prices changed",
                    10021: "No quotes",
                    10022: "Invalid expiration",
                    10023: "Order state changed",
                    10024: "Too frequent requests",
                    10025: "No changes in request",
                    10026: "Autotrading disabled by server",
                    10027: "Autotrading disabled by client",
                    10028: "Request locked",
                    10029: "Order frozen",
                }
                
                if result.retcode in error_guidance:
                    print(f"💡 {error_guidance[result.retcode]}")
                
                return None
            
            # SUCCESS!
            print(f"✅ ORDER EXECUTED SUCCESSFULLY!")
            print(f"   - Ticket: {result.order}")
            print(f"   - Price: {result.price}")
            print(f"   - Volume: {result.volume}")
            
            return result
                
        except Exception as e:
            print(f"❌ Exception in place_order: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return None
    
    async def close_position(self, ticket, volume=None, comment="Closed by bot"):
        """Close a position by ticket number"""
        try:
            print(f"🔧 Closing position {ticket}...")
            
            positions = mt5.positions_get(ticket=ticket)
            if not positions:
                print(f"❌ Position {ticket} not found")
                return None
            
            position = positions[0]
            symbol = position.symbol
            volume_to_close = volume if volume else position.volume
            
            print(f"📊 Closing {volume_to_close} lots of {symbol}")
            
            # Determine close order type (opposite of open)
            if position.type == mt5.ORDER_TYPE_BUY:
                close_type = mt5.ORDER_TYPE_SELL
                price = mt5.symbol_info_tick(symbol).bid
                print(f"🔴 Closing LONG at BID: {price}")
            else:
                close_type = mt5.ORDER_TYPE_BUY
                price = mt5.symbol_info_tick(symbol).ask
                print(f"🟢 Closing SHORT at ASK: {price}")
            
            # Shorten comment for close order too
            short_comment = comment[:25] if comment else "Close DCA"
            
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "position": ticket,
                "symbol": symbol,
                "volume": volume_to_close,
                "type": close_type,
                "price": price,
                "deviation": 20,
                "magic": position.magic,
                "comment": short_comment,
                "type_filling": mt5.ORDER_FILLING_IOC,
            }
            
            print(f"📤 Sending close request...")
            result = mt5.order_send(request)
            
            if result is None:
                print("❌ Close order returned None")
                return None
                
            print(f"📨 Close result: {result.retcode} - {result.comment}")
            
            # Success codes for closing
            success_codes = [mt5.TRADE_RETCODE_DONE, 10009, 10008]
            
            if result.retcode not in success_codes:
                print(f"❌ Close failed: {result.retcode} - {result.comment}")
                return None
            
            print(f"✅ Position closed successfully!")
            return result
            
        except Exception as e:
            print(f"❌ Error closing position: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return None
    
    async def get_open_positions(self, symbol=None):
        """Get open positions"""
        try:
            if symbol:
                positions = mt5.positions_get(symbol=symbol)
            else:
                positions = mt5.positions_get()
            
            return positions if positions else []
        except Exception as e:
            logging.error(f"Error getting open positions: {str(e)}")
            return []
    
    async def calculate_position_size(self, symbol, risk_percent=1, stop_loss_pips=50):
        """Calculate position size based on risk management"""
        try:
            balance_info = await self.get_balance()
            balance = balance_info['balance']
            
            if balance <= 0:
                logging.error(f"Invalid balance: {balance}")
                return 0.01
                
            risk_amount = balance * risk_percent / 100
            
            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info:
                logging.warning(f"No symbol info for {symbol}, using default lot size")
                return 0.01
            
            # Calculate pip value
            pip_value_per_lot = 10  # $10 per pip per standard lot
            
            # Calculate position size in lots
            if stop_loss_pips > 0:
                position_size = risk_amount / (stop_loss_pips * pip_value_per_lot)
            else:
                position_size = risk_amount / (50 * pip_value_per_lot)
            
            # Ensure minimum and maximum lot size
            min_lot = symbol_info.volume_min if hasattr(symbol_info, 'volume_min') else 0.01
            max_lot = symbol_info.volume_max if hasattr(symbol_info, 'volume_max') else 1.0
            
            position_size = max(min_lot, min(position_size, max_lot))
            
            # Round to step size
            step = symbol_info.volume_step if hasattr(symbol_info, 'volume_step') else 0.01
            if step > 0:
                position_size = round(position_size / step) * step
            
            logging.info(f"Position size for {symbol}: {position_size:.2f} lots (Risk: ${risk_amount:.2f}, SL: {stop_loss_pips} pips)")
            return position_size
            
        except Exception as e:
            logging.error(f"Error calculating position size for {symbol}: {str(e)}")
            return 0.01