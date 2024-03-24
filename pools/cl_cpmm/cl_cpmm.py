import math
import warnings
import pandas as pd

class ConcentratedLiquidity():
    """
    ConcentratedLiquidity implementation in Python to replay transactions and track LP profit.
    This class does not take into consideration the variable prescision as in the solidity implimentations
    """

    def __init__(self,
        token0,
        token1,
        poolAddress,
        fee,
        tickSpacing,
        protocol_fee = 0,
        ):
        """
        Parameters
        ----------
        token0  :   str
            Token address name for token0 in the pool
        token1  :   str
            Token address name for token0 in the pool
        poolAddress :   str
            Token address for the pool
        fee :   int
            Fee as in logs Initalise
        protocol_fee    :   float
            percentage of fee that goes to the protocol
        tickSpacing :   int
            tickspacing in the pool
        """
        
        self.token0 = token0
        self.token1 = token1
        self.poolAddress = poolAddress
        self.fee = fee*10**-6 #fee adjustment to get to a percentage fee
        self.tickSpacing = tickSpacing
        self.protocol_fee = protocol_fee*10**-6 #fee adjustment to get to a percentage fee
        self.positions = pd.DataFrame(columns = ['tokenId', 'last_L', 'start_L', 'increase_L', 
                                                   'tickLower', 'tickUpper', 'owner', 
                                                    'start_token0_holdings', 'start_token1_holdings',
                                                    'increase_token0_holdings', 'increase_token1_holdings',
                                                    'last_token0_holdings', 'last_token1_holdings',
                                                    'token0_fees_accrued', 'token1_fees_accrued',
                                                    'token0_collected', 'token1_collected',
                                                    'start_logIndex', 'start_blockNumber', 'start_transactionIndex', 'start_transactionHash', 
                                                    'last_logIndex', 'last_blockNumber', 'last_transactionIndex', 'last_transactionHash'])
        self.mints = pd.DataFrame()
        self.burns = pd.DataFrame()
        self.collects = pd.DataFrame()
        self.swaps = pd.DataFrame()
        self.total_fee0 = 0
        self.total_fee1 = 0
        self.liquidity = 0
        self.Q96 = 2**96
        self.sqrtPrice = None 
        self.sqrtPriceX96 = None

    
    def Initialize(self, 
                   sqrtPrice = None, 
                   sqrtPriceX96 = None, 
                   price = None, 
                   tick = None,
                   warn = False
                   ):
        """
        Add Initialize event to pool object.
        Updates pool state for new price and tick
        Validates the inputs are correct

        Parameters
        ----------
        sqrtPrice  :   float
            Initialized sqrtPrice for the pool this input takes precident over sqrtPriceX96 and price
        sqrtPriceX96 :   str
            Initialized sqrtPrice for the pool this input takes precident over price
        price :   int
            Initialized price for the pool
        tick  :   int
            Initialized tick for the pool
        """
                

        if not any([sqrtPrice, sqrtPriceX96, price]):
            raise IncorrectInput("Initialize: need to add a sqrtPrice, price or sqrtPriceX96")

        if price is not None:
            self.sqrtPrice = price**0.5
            self.sqrtPriceX96 = sqrtPrice*self.Q96
        elif sqrtPriceX96 is not None:
            self.sqrtPriceX96 = sqrtPriceX96
            self.sqrtPrice = float(self.sqrtPriceX96_to_sqrtPrice(sqrtPriceX96))
        elif sqrtPrice is not None:
            self.sqrtPrice = float(sqrtPrice)
            self.sqrtPriceX96 = sqrtPrice*self.Q96
        
        #check if tick is supplied, otherwise use the self calc. remove keep data, can not supply tick if dont want check
        if tick is not None:
            if self.sqrtPrice_to_tick(self.sqrtPrice) != tick:
                if warn:
                    TickPriceAllignmentError(f"Initialize: The price and tick supplied do not match\n\n\t{tick}, {self.sqrtPrice_to_tick(self.sqrtPrice)}")
                else:
                    raise TickPriceAllignmentError(f"Initialize: The price and tick supplied do not match\n\n\t{tick}, {self.sqrtPrice_to_tick(self.sqrtPrice)}")
            else:
                self.tick = tick
        else:
            self.tick = self.sqrtPrice_to_tick(self.sqrtPrice)
        return


    def Mint(self, tickLower, tickUpper, amount, amount0, amount1, sender, blockNumber, transactionIndex, logIndex, transactionHash, tokenId):
        """
        Add Mint event to pool object.
        Updates pool state for an increase in liquidity (L) and reserves
        #TODO add validation on amount

        Parameters
        ----------
        tickLower  :   float
            The tickLower for the mint event emited by the pool
        tickUpper  :   float
            The tickUpper for the mint event emited by the pool
        amount  :   float
            The amount of liquidity added in the mint event emited by the pool
        amount0  :   float
            The amount of token0 added in the mint event emited by the pool
        amount1  :   float
            The amount of token1 added in the mint event emited by the pool
        sender  :   str
            The address of the sender in the mint event emited by the pool
        blockNumber  :   int
            The blockNumber of the mint event emited by the pool
        transactionIndex  :   int
            The transactionIndex of the mint event emited by the pool
        logIndex  :   int
            The logIndex of the mint event emited by the pool
        transactionHash  :   str
            The transactionHash of the mint event emited by the pool
        tokenId  :   int
            The tokenId of the increaseLiquidity event emited by the nft manager
        """

        
        mint_df = pd.DataFrame([['Mint', logIndex, blockNumber, transactionIndex, transactionHash, sender, amount, tickLower, tickUpper, amount0, amount1, tokenId]], 
                       columns=['event', 'logIndex', 'blockNumber', 'transactionIndex', 'transactionHash', 'sender', 'amount', 'tickLower', 'tickUpper', 'amount0', 'amount1', 'tokenId'])
        
        self.mints = pd.concat([self.mints, mint_df])

        pos = self.positions      

        if tokenId in list(pos['tokenId']):
            pos['last_L'] = pos['last_L'].mask(pos['tokenId'] == tokenId, pos['last_L'] + amount)
            pos['last_token0_holdings'] = pos['last_token0_holdings'].mask(pos['tokenId'] == tokenId, pos['last_token0_holdings'] + amount0)
            pos['last_token1_holdings'] = pos['last_token1_holdings'].mask(pos['tokenId'] == tokenId, pos['last_token1_holdings'] + amount1)
            pos['increase_L'] = pos['last_L'].mask(pos['tokenId'] == tokenId, pos['last_L'] + amount)
            pos['increase_token0_holdings'] = pos['increase_token0_holdings'].mask(pos['tokenId'] == tokenId, pos['increase_token0_holdings'] + amount0)
            pos['increase_token1_holdings'] = pos['increase_token1_holdings'].mask(pos['tokenId'] == tokenId, pos['increase_token1_holdings'] + amount1) #tracks when a mint is for the same position not driven by price changes
            pos = self.position_last_update_state(pos, blockNumber, transactionIndex, logIndex, transactionHash)
            self.positions = pos

        else:
            add_active_df = pd.DataFrame([[tokenId, float(amount), float(amount), float(0), tickLower, tickUpper, sender,
                                        float(amount0), float(amount1), 
                                        float(0), float(0),
                                        float(amount0), float(amount1),
                                        float(0),float(0),float(0),float(0),
                                        logIndex, blockNumber, transactionIndex, transactionHash,
                                        logIndex, blockNumber, transactionIndex, transactionHash]],
                                        columns = ['tokenId', 'last_L', 'start_L', 'increase_L', 
                                                   'tickLower', 'tickUpper', 'owner', 
                                                    'start_token0_holdings', 'start_token1_holdings',
                                                    'increase_token0_holdings', 'increase_token1_holdings',
                                                    'last_token0_holdings', 'last_token1_holdings',
                                                    'token0_fees_accrued', 'token1_fees_accrued',
                                                    'token0_collected', 'token1_collected',
                                                    'start_logIndex', 'start_blockNumber', 'start_transactionIndex', 'start_transactionHash', 
                                                    'last_logIndex', 'last_blockNumber', 'last_transactionIndex', 'last_transactionHash'])

            if pos.empty:
                self.positions = add_active_df
            else:
                self.positions = pd.concat([pos, add_active_df]).reset_index(drop=True)

        #save intick liquidity
        pos = self.positions
        current_tick = self.sqrtPrice_to_tick(self.sqrtPrice)
        self.liquidity = pos['last_L'].loc[(pos['tickLower'] <= current_tick)&(pos['tickUpper'] > current_tick)].sum()
        return

        
    def Burn(self, tickLower, tickUpper , amount, amount0, amount1, owner, blockNumber, transactionIndex, logIndex, transactionHash, tokenId):
        """
        Add Burn event to pool object.
        Updates pool state for a decrease in liquidity (L) 
        Raises BurnMintMatchError when position cannot be identified
        Warns when the liquidity amount burned creates a position with negative liquidity. Forces liquidity to always be positive
        #TODO add validation on amounts 

        Parameters
        ----------
        tickLower  :   float
            The tickLower for the burn event emited by the pool
        tickUpper  :   float
            The tickUpper for the burn event emited by the pool
        amount  :   float
            The amount of liquidity added in the burn event emited by the pool
        amount0  :   float
            The amount of token0 added in the burn event emited by the pool
        amount1  :   float
            The amount of token1 added in the burn event emited by the pool
        sender  :   str
            The address of the sender in the burn event emited by the pool
        blockNumber  :   int
            The blockNumber of the burn event emited by the pool
        transactionIndex  :   int
            The transactionIndex of the burn event emited by the pool
        logIndex  :   int
            The logIndex of the burn event emited by the pool
        transactionHash  :   str
            The transactionHash of the burn event emited by the pool
        tokenId  :   int
            The tokenId of the decreaseLiquidity event emited by the nft manager
        """

        burn_df = pd.DataFrame([['Burn', logIndex, blockNumber, transactionIndex, transactionHash, owner, amount, tickLower, tickUpper, amount0, amount1, tokenId]], 
                       columns=['event', 'logIndex', 'blockNumber', 'transactionIndex', 'transactionHash', 'sender', 'amount', 'tickLower', 'tickUpper', 'amount0', 'amount1', 'tokenId'])
        
        self.burns = pd.concat([self.burns, burn_df])
        
        pos = self.positions

        bpos = pos.loc[pos['tokenId'] == tokenId]

        if len(bpos) != 1:
            raise BurnMintMatchError(f"Cannot match burn with active position. There are {len(bpos)} positions that match the tokenId")
        
        pos['last_L'] = pos['last_L'].mask(pos['tokenId'] == tokenId, pos['last_L'] - amount)
        pos['last_token0_holdings'] = pos['last_token0_holdings'].mask(pos['tokenId'] == tokenId, amount0)
        pos['last_token1_holdings'] = pos['last_token1_holdings'].mask(pos['tokenId'] == tokenId, amount1)
        pos = self.position_last_update_state(pos, blockNumber, transactionIndex, logIndex, transactionHash)

        if not pos.loc[pos['last_L'] < 0].empty:
            warnings.warn(f"\nBurn event resulted in negative liquidity. Has been set to 0")
            pos['last_L'] = pos['last_L'].mask(pos['last_L'] < 0, 0)

        self.positions = pos.copy()
        return 
    
    
    def Collect(self, tickLower, tickUpper, amount0, amount1, recipient, blockNumber, transactionIndex, logIndex, transactionHash, tokenId):
        """
        Add Collect event to pool object.
        Updates pool state for a decrease in reserves
        Raises CollectMatchError when position cannot be identified


        Parameters
        ----------

        tickLower  :   float
            The tickLower for the collect event emited by the pool
        tickUpper  :   float
            The tickUpper for the collect event emited by the pool
        amount  :   float
            The amount of liquidity added in the collect event emited by the pool
        amount0  :   float
            The amount of token0 added in the collect event emited by the pool
        amount1  :   float
            The amount of token1 added in the collect event emited by the pool
        recipient  :   str
            The address of the recipient in the collect event emited by the pool
        blockNumber  :   int
            The blockNumber of the collect event emited by the pool
        transactionIndex  :   int
            The transactionIndex of the collect event emited by the pool
        logIndex  :   int
            The logIndex of the collect event emited by the pool
        transactionHash  :   str
            The transactionHash of the collect event emited by the pool
        tokenId  :   int
            The tokenId of the collect event emited by the nft manager
        """

        collect_df = pd.DataFrame([['Collect', logIndex, blockNumber, transactionIndex, transactionHash, recipient, tickLower, tickUpper, amount0, amount1, tokenId]], 
                       columns=['event', 'logIndex', 'blockNumber', 'transactionIndex', 'transactionHash', 'sender', 'tickLower', 'tickUpper', 'amount0', 'amount1', 'tokenId'])
        self.collects = pd.concat([self.collects, collect_df])

        pos = self.positions

        cpos = pos.loc[pos['tokenId'] == tokenId]
        if len(cpos) != 1:
            CollectMatchError(f"Cannot match Collect with active position. There are {len(cpos)} positions that match the tokenId") 
        
        pos['token0_collected'] = pos['token0_collected'].mask(pos['tokenId'] == tokenId, pos['token0_collected'] + amount0)
        pos['token1_collected'] = pos['token1_collected'].mask(pos['tokenId'] == tokenId, pos['token1_collected'] + amount1)

        pos[['last_token0_holdings', 'last_token1_holdings']] = pos.apply(lambda x: self.get_amounts(self.sqrtPrice, self.tick_to_sqrtPrice(x.tickLower), self.tick_to_sqrtPrice(x.tickUpper), x.last_L), axis = 1, result_type='expand')
        pos = self.position_last_update_state(pos, blockNumber, transactionIndex, logIndex, transactionHash)
        self.positions = pos.copy()
        return
    
    def Swap(self, amount0, amount1,  sender, recipient, logIndex, blockNumber, transactionIndex, transactionHash, sqrtPriceX96 = None, tick = None, liquidity = None, warn_all = False, tolerance = 0.025, pass_error = False):
        """
        Add Swap event to pool object.
        Updates pool state for swap event
        Updates the fees collected and real reserves for each position
        Raises SwapAllignmentError when swap inputs do not match. Can be ignored or warning based on tolerance

        Parameters
        ----------
        amount0  :   float
            The amount of token0 in the swap event emited by the pool
        amount1  :   float
            The amount of token1 in the swap event emited by the pool
        sender  :   str
            The address of the sender in the swap event emited by the pool
        recipient  :   str
            The address of the recipient in the swap event emited by the pool
        blockNumber  :   int
            The blockNumber of the swap event emited by the pool
        transactionIndex  :   int
            The transactionIndex of the swap event emited by the pool
        logIndex  :   int
            The logIndex of the swap event emited by the pool
        transactionHash  :   str
            The transactionHash of the swap event emited by the pool
        sqrtPriceX96  :   int
            The sqrtPriceX96 of the swap event emited by the pool
        tick  :   int
            The tick of the swap event emited by the pool
        liquidity  :   int
            The liquidity of the swap event emited by the pool
        warn_all  :   bool
            Set to True to remove any raise of the errors
        tolerance  :   float
            The percentage of tolerance of variation allowed between tick and liquidity, 
            if within the tolerance percentage warns that there is a difference,
            otherwise raises the SwapAllignmentError
        pass_error  :   bool
            Set to True to remove any raise of the errors/warnings
        """

        swap_df = pd.DataFrame([['Swap', logIndex, blockNumber, transactionIndex, transactionHash, sender, recipient, amount0, amount1, sqrtPriceX96, tick, liquidity]], 
                       columns=['event', 'logIndex', 'blockNumber', 'transactionIndex', 'transactionHash', 'sender', 'recipient', 'amount0', 'amount1', 'sqrtPriceX96', 'tick', 'liquidity'])
        
        self.swaps = pd.concat([self.swaps, swap_df])

        #Fees for pool 
        zeroForOne = None
        if amount0 > 0:
            total_fee0 = amount0*self.fee
            amount0_nf = amount0 - total_fee0
            zeroForOne = True
        else:
            total_fee0 = 0
            amount0_nf = amount0

        if amount1 > 0:
            total_fee1= amount1*self.fee
            amount1_nf = amount1 - total_fee1
            zeroForOne = False
        else:
            total_fee1 = 0
            amount1_nf = amount1

        if zeroForOne == None:
            raise SwapAmountError(f"\nSwap amounts are incorrect:\n\n\t{(amount0, amount1)}\n")

        self.total_fee0 += total_fee0
        self.total_fee1 += total_fee1

        pos = self.positions
        
        #check that the active positions cover the current tick, otherwise move tick to the closest liquidity or down to lowest
        active_pos = pos.loc[(pos['last_L'] > 0)]
        lowest_tick = active_pos['tickLower'].min()
        highest_tick = active_pos['tickUpper'].max()

        if lowest_tick > self.tick:
            self.tick = lowest_tick

        if highest_tick < self.tick:
            self.tick = highest_tick

        current_tick, current_tick_lower, current_tick_upper = self.get_tick_range(self.tick)


        if zeroForOne:
            #print(f'{current_tick}, {current_tick_lower}')
            sqrtPrice = self.sqrtPrice 
            sqrtPriceA = self.tick_to_sqrtPrice(current_tick_lower) #based on lower tick not lower price

            amount0_a = amount0_nf
            amount1_a = amount1_nf

            fee0_collected = 0
            while amount0_a > 0:
                active_pos = pos.loc[(pos['tickLower'] < current_tick)&(pos['tickUpper'] >= current_tick)&(pos['last_L'] > 0)]
                
                if active_pos.empty:
                    if current_tick == pos['tickLower'].loc[(pos['last_L'] > 0)].min():
                        active_pos = pos.loc[(pos['tickLower'] == current_tick)&(pos['last_L'] > 0)]
                    elif current_tick < pos['tickLower'].loc[(pos['last_L'] > 0)].min(): 
                        current_tick = pos['tickLower'].loc[(pos['last_L'] > 0)].min()
                        current_tick_lower = current_tick - self.tickSpacing
                        continue
                    else:    
                        current_tick = current_tick_lower
                        current_tick_lower = current_tick - self.tickSpacing
                        continue

                L = active_pos['last_L'].sum()
                
                #check if there is enough reserves in the tick
                if self.get_amount0(sqrtPrice, sqrtPriceA, L) > amount0_a:
                    sqrtPrice_next = self.get_next_sqrtPrice_from_inputs(sqrtPrice, L, amount0_a, zeroForOne=zeroForOne)
                    tick_next = self.sqrtPrice_to_tick(sqrtPrice_next)
                    fee0_in_range = round((amount0_a/(1-self.fee)) - amount0_a)
                    fee0_collected += fee0_in_range
                    fee0_per_L = fee0_in_range/L

                    pos['token0_fees_accrued'] = pos['token0_fees_accrued'].mask(pos.index.isin(active_pos.index), pos['token0_fees_accrued'] + (pos['last_L'] * fee0_per_L))
                    break

                else:
                    amount0_diff = self.get_amount0(sqrtPrice, sqrtPriceA, L) 
                    amount1_diff = self.get_amount1(sqrtPrice, sqrtPriceA, L) 

                    fee0_in_range = round((amount0_diff/(1-self.fee)) - amount0_diff)
                    fee0_collected += fee0_in_range
                    fee0_per_L = fee0_in_range/L
                    pos['token0_fees_accrued'] = pos['token0_fees_accrued'].mask(pos.index.isin(active_pos.index), pos['token0_fees_accrued'] + (pos['last_L'] * fee0_per_L))

                    amount0_a -= amount0_diff
                    amount1_a -= amount1_diff

                    current_tick = current_tick_lower 
                    current_tick_lower = current_tick - self.tickSpacing
                    sqrtPrice = self.tick_to_sqrtPrice(current_tick)
                    sqrtPriceA = self.tick_to_sqrtPrice(current_tick_lower)

        else:
            #print(f'{current_tick}, {current_tick_upper}')
            sqrtPrice = self.sqrtPrice
            sqrtPriceB = self.tick_to_sqrtPrice(current_tick_upper) #based on upper tick not upper price

            amount0_a = amount0_nf
            amount1_a = amount1_nf

            fee1_collected = 0
            while amount1_a > 0:
                active_pos = pos.loc[(pos['tickLower'] <= current_tick)&(pos['tickUpper'] > current_tick)&(pos['last_L'] > 0)]
                
                if active_pos.empty:
                    if current_tick == pos['tickUpper'].loc[(pos['last_L'] > 0)].max():
                        active_pos = pos.loc[(pos['tickUpper'] == current_tick)&(pos['last_L'] > 0)]

                    elif current_tick > pos['tickUpper'].loc[(pos['last_L'] > 0)].max(): 
                        current_tick = pos['tickUpper'].loc[(pos['last_L'] > 0)].max()
                        current_tick_upper = current_tick + self.tickSpacing
                        continue

                    else:    
                        current_tick = current_tick_upper
                        current_tick_upper = current_tick + self.tickSpacing
                        continue


                if active_pos.empty: #if no liquidity, skip to next tick #current tick to next tick bound
                    #check if there is any liquidity above
                    if current_tick > pos['tickUpper'].loc[(pos['last_L'] > 0)].max():
                        current_tick = pos['tickLower'].loc[(pos['last_L'] > 0)].max()
                    else:    
                        current_tick = current_tick_upper

                    current_tick_upper = current_tick + self.tickSpacing
                    continue

                L = active_pos['last_L'].sum()

                #check if there is enough reserves in the tick
                if self.get_amount1(sqrtPrice, sqrtPriceB, L) > amount1_a:
                    sqrtPrice_next = self.get_next_sqrtPrice_from_inputs(sqrtPrice, L, amount1_a, zeroForOne=zeroForOne)
                    tick_next = self.sqrtPrice_to_tick(sqrtPrice_next)

                    fee1_in_range = round((amount1_a/(1-self.fee)) - amount1_a)
                    fee1_collected += fee1_in_range
                    fee1_per_L = fee1_in_range/L

                    pos['token1_fees_accrued'] = pos['token1_fees_accrued'].mask(pos.index.isin(active_pos.index), pos['token1_fees_accrued'] + (pos['last_L'] * fee1_per_L))
                    break

                else:
                    amount0_diff = self.get_amount0(sqrtPrice, sqrtPriceB, L) 
                    amount1_diff = self.get_amount1(sqrtPrice, sqrtPriceB, L) 

                    fee1_in_range = round((amount1_diff/(1-self.fee)) - amount1_diff)
                    fee1_collected += fee1_in_range
                    fee1_per_L = fee1_in_range/L
                    pos['token1_fees_accrued'] = pos['token1_fees_accrued'].mask(pos.index.isin(active_pos.index), pos['token1_fees_accrued'] + (pos['last_L'] * fee1_per_L))

                    amount0_a -= amount0_diff
                    amount1_a -= amount1_diff

                    current_tick = current_tick_upper
                    current_tick_upper = current_tick + self.tickSpacing
                    sqrtPrice = self.tick_to_sqrtPrice(current_tick)
                    sqrtPriceB = self.tick_to_sqrtPrice(current_tick_upper)


        if not any([liquidity, tick, sqrtPriceX96]): #save if check not given
            self.sqrtPrice = sqrtPrice_next
            self.tick = tick_next
            self.liquidity = L

        elif pass_error:
            self.sqrtPrice = self.sqrtPriceX96_to_sqrtPrice(sqrtPriceX96)
            self.tick = tick
            self.liquidity = pos['last_L'].loc[(pos['tickLower'] < current_tick)&(pos['tickUpper'] >= current_tick)&(pos['last_L'] > 0)].sum()

        else:
            if warn_all:
                if int(tick) != int(tick_next):
                    warnings.warn(f"\n\nSwap: tick provided does not match calculations\n\n\ttick: {int(tick)}, {int(tick_next)}\n")
                elif L != liquidity:
                    warnings.warn(f"\n\nSwap: liquidity provided does not match calculations\n\n\tliquidity: {int(L)}, {int(liquidity)}\n")
                
            else:
                if int(tick) != int(tick_next):
                    if not (math.ceil(tick+(tolerance*100)) >= tick_next) and (math.floor(tick-(tolerance*100)) <= tick_next): #ticks are in bips for tolerance
                        raise SwapAllignmentError(f"\n\nSwap: tick provided does not match calculations\n\n\ttick: {int(tick)}, {int(tick_next)}\n")
                    else: 
                        warnings.warn(f"\n\nSwap: tick provided does not match calculations\n\n\ttick: {int(tick)}, {int(tick_next)}\n")

                elif L != liquidity:
                    if not (liquidity*(1+tolerance) >= L) and (liquidity*(1-tolerance) <= L):
                        raise SwapAllignmentError(f"\n\nSwap: liquidity provided does not match calculations\n\n\tliquidity: {int(L)}, {int(liquidity)}\n")
                    else: 
                        warnings.warn(f"\n\nSwap: liquidity provided does not match calculations\n\n\tliquidity: {int(L)}, {int(liquidity)}\n")

            #self.sqrtPrice = sqrtPrice_next 
            #self.tick = tick_next
            self.sqrtPrice = self.sqrtPriceX96_to_sqrtPrice(sqrtPriceX96) #use the supplied tick for next price to avoid carry forward errors
            self.tick = tick
            self.liquidity = L 

        #Update positions for estimate portfolio holdings 
        #It is an estimate due to precision errors but close enough for estimation of profit
        #Reset when tokens are burnt in the contract taking the logs value
        pos[['last_token0_holdings', 'last_token1_holdings']] = pos.apply(lambda x: self.get_amounts(self.sqrtPrice, self.tick_to_sqrtPrice(x.tickLower), self.tick_to_sqrtPrice(x.tickUpper), x.last_L), axis = 1, result_type='expand')
        pos = self.position_last_update_state(pos, blockNumber, transactionIndex, logIndex, transactionHash)
        self.positions = pos.copy()
        return
    
    def get_active_LP_positions(self):
        """
        View function for all active liquidity provider positions.
        The positions that have positive liquidity

        Returns
        -------
        DataFrame
            Dataframe of all active liquidity positions
        """

        positions = self.positions
        return positions.loc[positions['last_L'] > 0]
    
    def view_all_pool_events(self):
        """
        View function for all pool events supplied.

        Returns
        -------
        DataFrame
            Dataframe of all pool events that have updated the state in order
        """

        pool_events = pd.concat([self.mints, self.burns, self.collects, self.swaps])
        if pool_events.empty:
            return  pool_events
        else:
            return pool_events.sort_values(['blockNumber', 'logIndex']).reset_index(drop=True)

    def sqrtPriceX96_to_sqrtPrice(self, sqrtPriceX96):
        """
        Convert sqrtPriceX96 to sqrtPrice

        Parameters
        ----------
        sqrtPriceX96  :   int
            The sqrtPriceX96 to be converted

        Returns
        -------
        float
            sqrtPrice
        """

        return float(sqrtPriceX96 / self.Q96)
    
    def sqrtPrice_to_tick(self, sqrtPrice):
        """
        Convert sqrtPrice to tick

        Parameters
        ----------
        sqrtPrice  :   float
            The sqrtPrice to be converted

        Returns
        -------
        int
            tick of sqrtPrice
        """

        return math.floor(round(math.log(sqrtPrice, math.sqrt(1.0001)), 6)) #control for precision issues and tick int size with the rounding

    def sqrtPrice_to_tick_rounding(self, sqrtPrice):
        """
        Convert sqrtPrice to tick based on the solidity rounding convention towards 0

        Parameters
        ----------
        sqrtPrice  :   float
            The sqrtPrice to be converted

        Returns
        -------
        int
            tick of sqrtPrice
        """

        #solidity rounds towards 0
        temp_tick = round(math.log(sqrtPrice, math.sqrt(1.0001)), 6) #control for precision issues and tick int size with the rounding
        if temp_tick < 0:
            return math.ceil(temp_tick)
        else: 
            return math.floor(temp_tick)
    
    def tick_to_sqrtPrice(self, tick):
        """
        Convert tick to sqrtPrice 

        Parameters
        ----------
        tick  :   int
            The tick to be converted

        Returns
        -------
        float
            sqrtPrice of tick
        """

        return float(1.0001 ** (tick / 2))
    
    def price(self):
        """
        View function to get the current price of token1 from the sqrtPrice

        Returns
        -------
        float
            Price of token1
        """
        

        return self.sqrtPrice**2

    def get_tick_range(self, tick):
        """
        Get the current tick range based on the initalized tick spacing 

        Parameters
        ----------
        tick  :   int
            The tick for the tick range to be calculated

        Returns
        -------
        tuple
            returns a tuple of tick ranges (current_tick, lower_tick, upper_tick)
        """
                
        return tick, (tick//self.tickSpacing * self.tickSpacing), tick//self.tickSpacing * self.tickSpacing + self.tickSpacing
    
    def position_last_update_state(self, position, blockNumber, transactionIndex, logIndex, transactionHash):
        """
        Function to update the last state tracking of the events

        Parameters
        ----------
        position  :   DataFrame
            The dataframe of positions for the state to be updated
        blockNumber  :   int
            The blockNumber of the event emited by the pool
        transactionIndex  :   int
            The transactionIndex of the event emited by the pool
        logIndex  :   int
            The logIndex of the event emited by the pool
        transactionHash  :   str
            The transactionHash of the event emited by the pool
        
        Returns
        -------
        DataFrame
            The dataframe of positions with the updated state
        """

        position['last_blockNumber'] = blockNumber
        position['last_transactionIndex'] = transactionIndex
        position['last_logIndex'] = logIndex
        position['last_transactionHash'] = transactionHash
        return position
    
    def get_amount0(self, sqrtPriceA, sqrtPriceB, L):
        """
        Get the amount of token0 between the two prices supplied for an amount of liquidity L
        Order of prices is handled internally

        Parameters
        ----------
        sqrtPriceA  :   float
            The lower sqrtPrice of the range
        sqrtPriceB  :   float
            The upper sqrtPrice of the range
        L  :   float
            The amount of liquidity in the range

        Returns
        -------
        float
            amount of token0 in the range
        """

        if sqrtPriceA > sqrtPriceB:
            sqrtPriceA, sqrtPriceB = sqrtPriceB, sqrtPriceA
        
        return L * ((1/sqrtPriceA) - (1/sqrtPriceB))

    def get_amount1(self, sqrtPriceA, sqrtPriceB, L):
        """
        Get the amount of token1 between the two prices supplied for an amount of liquidity L
        Order of prices is handled internally

        Parameters
        ----------
        sqrtPriceA  :   float
            The lower sqrtPrice of the range
        sqrtPriceB  :   float
            The upper sqrtPrice of the range
        L  :   float
            The amount of liquidity in the range

        Returns
        -------
        float
            amount of token1 in the range
        """

        if sqrtPriceA > sqrtPriceB:
            sqrtPriceA, sqrtPriceB = sqrtPriceB, sqrtPriceA
        
        return (L * (sqrtPriceB - sqrtPriceA))

    def get_amounts(self, sqrtPrice, sqrtPriceA, sqrtPriceB, L):
        """
        Get the amount of token0 and token1 between the two prices supplied for an amount of liquidity L and current sqrtPrice
        Order of prices is handled internally

        Parameters
        ----------
        sqrtPrice  :   float
            The current sqrtPrice in the pool
        sqrtPriceA  :   float
            The lower sqrtPrice of the range
        sqrtPriceB  :   float
            The upper sqrtPrice of the range
        L  :   float
            The amount of liquidity in the range

        Returns
        -------
        tuple
            returns a tuple of floats with amount0 and amount1            
        """


        amount0 = 0
        amount1 = 0

        if sqrtPriceA > sqrtPriceB:
            sqrtPriceA, sqrtPriceB = sqrtPriceB, sqrtPriceA
        
        if sqrtPrice <= sqrtPriceA:
            amount0 = self.get_amount0(sqrtPriceA, sqrtPriceB, L)
        
        elif sqrtPrice < sqrtPriceB:
            amount0 = self.get_amount0(sqrtPrice, sqrtPriceB, L)
            amount1 = self.get_amount1(sqrtPriceA, sqrtPrice, L)
        
        else:
            amount1 = self.get_amount1(sqrtPriceA, sqrtPriceB, L)

        return amount0, amount1

    def get_next_sqrtPrice_from_amount0(self, sqrtPrice, L, amonutIn): 
        """
        Get the next sqrtPrice from amount0

        Parameters
        ----------
        sqrtPrice  :   float
            The current sqrtPrice in the pool
        L  :   float
            The amount of liquidity in the range
        amonutIn  :   float
            The amount of token0 being added to the pool

        Returns
        -------
        float
            next sqrtPrice in the pool  
        """

        return 1/((amonutIn/L)+(1/sqrtPrice))
    
    def get_next_sqrtPrice_from_amount1(self, sqrtPrice, L, amonutIn): 
        """
        Get the next sqrtPrice from amount1

        Parameters
        ----------
        sqrtPrice  :   float
            The current sqrtPrice in the pool
        L  :   float
            The amount of liquidity in the range
        amonutIn  :   float
            The amount of token1 being added to the pool

        Returns
        -------
        float
            next sqrtPrice in the pool  
        """

        return sqrtPrice + (amonutIn / L)
    
    def get_next_sqrtPrice_from_inputs(self, sqrtPrice, L, amonutIn, zeroForOne):
        """
        Get the next sqrtPrice from input amount

        Parameters
        ----------
        sqrtPrice  :   float
            The current sqrtPrice in the pool
        L  :   float
            The amount of liquidity in the range
        amonutIn  :   float
            The amount of tokens being added to the pool
        zeroForOne  :   bool
            True if the swap is for token0 being added to the pool otherwise token1
        
        Returns
        -------
        float
            next sqrtPrice in the pool  
        """

        if zeroForOne:
            sqrtPriceNext = self.get_next_sqrtPrice_from_amount0(sqrtPrice, L, amonutIn)
        else:
            sqrtPriceNext = self.get_next_sqrtPrice_from_amount1(sqrtPrice, L, amonutIn)
        return sqrtPriceNext

    def calc_L_from_amount0(self, amount, sqrtPriceA, sqrtPriceB):
        """
        Get the amount of liquidity from amount of token0 between price range
        Order of prices is handled internally

        Parameters
        ----------
        amount  :   float
            The amount of token0 being added to the pool
        sqrtPriceA  :   float
            The lower sqrtPrice of the range
        sqrtPriceB  :   float
            The upper sqrtPrice of the range

        Returns
        -------
        float
            liquidity
        """

        if sqrtPriceA > sqrtPriceB:
            sqrtPriceA, sqrtPriceB = sqrtPriceB, sqrtPriceA
        return amount/((1/sqrtPriceB)-(1/sqrtPriceA))

    def calc_L_from_amount1(self, amount, sqrtPriceA, sqrtPriceB):
        """
        Get the amount of liquidity from amount of token1 between price range
        Order of prices is handled internally

        Parameters
        ----------
        amount  :   float
            The amount of token1 being added to the pool
        sqrtPriceA  :   float
            The lower sqrtPrice of the range
        sqrtPriceB  :   float
            The upper sqrtPrice of the range

        Returns
        -------
        float
            liquidity
        """

        if sqrtPriceB > sqrtPriceA:
            sqrtPriceB, sqrtPriceA = sqrtPriceA, sqrtPriceB
        return amount / (sqrtPriceA - sqrtPriceB)

    def calc_L_from_amounts(self, amount0, amount1, sqrtPrice, sqrtPriceA, sqrtPriceB): #only estimates due to precision errors
        """
        Get the amount of liquidity from amounts of tokens between price range given current price
        Order of prices is handled internally

        Parameters
        ----------
        amount0  :   float
            The amount of token0 being added to the pool
        amount1  :   float
            The amount of token1 being added to the pool
        sqrtPrice  :   float
            The current sqrtPrice
        sqrtPriceA  :   float
            The lower sqrtPrice of the range
        sqrtPriceB  :   float
            The upper sqrtPrice of the range
        
        Returns
        -------
        int
            liquidity added in the pool
        """

        liquidity0 = self.calc_L_from_amount0(amount0, sqrtPrice, sqrtPriceB)
        liquidity1 = self.calc_L_from_amount1(amount1, sqrtPrice, sqrtPriceA)

        L = min(liquidity0, liquidity1)

        return int(L)
    
    def replay_from_logs_for_LP_profit(self, df, tolerance = 0.01, pass_error = False):
        """
        Replay the pool events from a Dataframe to calculate the profit of

        Parameters
        ----------
        df  :   DataFrame
            Dataframe of pool events in order 
        tolerance  :   float
            The percentage of tolerance of variation allowed between tick and liquidity, 
            if within the tolerance percentage warns that there is a difference,
            otherwise raises the SwapAllignmentError
        pass_error  :   bool
            Set to True to remove any raise of the errors/warnings

        Returns
        -------
        DataFrame
            Dataframe of each tick update to liquidity positions
            
        """

        pos_dfs = []
        for i in range(len(df)):
            tdf = df.iloc[i]

            if tdf['event'] == 'Initialize':
                if pass_error:
                    self.Initialize(sqrtPriceX96 = tdf['args.sqrtPriceX96'], 
                            tick = tdf['args.tick'], warn = False)
                else:
                    self.Initialize(sqrtPriceX96 = tdf['args.sqrtPriceX96'], 
                            tick = tdf['args.tick'])

            if pass_error:
                if tdf['event'] == 'Swap':
                    self.Swap(blockNumber = tdf['blockNumber'],
                            transactionIndex = tdf['transactionIndex'],
                            logIndex = tdf['logIndex'],
                            transactionHash = tdf['transactionHash'],
                            sender = tdf['args.sender'],
                            recipient = tdf['args.recipient'],
                            amount0 = tdf['args.amount0'],
                            amount1 = tdf['args.amount1'],
                            sqrtPriceX96 = tdf['args.sqrtPriceX96'],
                            tick = tdf['args.tick'],
                            liquidity = tdf['args.liquidity'],
                            pass_error = pass_error,
                            tolerance = tolerance)
            else:
                if tdf['event'] == 'Swap':
                    self.Swap(blockNumber = tdf['blockNumber'],
                            transactionIndex = tdf['transactionIndex'],
                            logIndex = tdf['logIndex'],
                            transactionHash = tdf['transactionHash'],
                            sender = tdf['args.sender'],
                            recipient = tdf['args.recipient'],
                            amount0 = tdf['args.amount0'],
                            amount1 = tdf['args.amount1'],
                            sqrtPriceX96 = tdf['args.sqrtPriceX96'],
                            tick = tdf['args.tick'],
                            liquidity = tdf['args.liquidity'],
                            tolerance=tolerance)


            if tdf['event'] == 'Collect':
                self.Collect(tickLower = tdf['args.tickLower'], 
                            tickUpper = tdf['args.tickUpper'], 
                            amount0 = tdf['args.amount0'],
                            amount1 = tdf['args.amount1'],
                            recipient = tdf['args.recipient'],
                            blockNumber = tdf['blockNumber'], 
                            transactionIndex = tdf['transactionIndex'], 
                            logIndex = tdf['logIndex'], 
                            transactionHash = tdf['transactionHash'],
                            tokenId = tdf['tokenId'])
                
            if tdf['event'] == 'Burn':
                self.Burn(tickLower = tdf['args.tickLower'], 
                        tickUpper = tdf['args.tickUpper'], 
                        amount = tdf['args.amount'],
                        amount0 = tdf['args.amount0'],
                        amount1 = tdf['args.amount1'],
                        owner = tdf['args.owner'],
                        blockNumber = tdf['blockNumber'], 
                        transactionIndex = tdf['transactionIndex'], 
                        logIndex = tdf['logIndex'], 
                        transactionHash = tdf['transactionHash'],
                        tokenId = tdf['tokenId'])

            if tdf['event'] == 'Mint':
                self.Mint(tickLower = tdf['args.tickLower'], 
                        tickUpper = tdf['args.tickUpper'], 
                        amount = tdf['args.amount'],
                        amount0 = tdf['args.amount0'],
                        amount1 = tdf['args.amount1'],
                        sender = tdf['args.sender'],
                        blockNumber = tdf['blockNumber'], 
                        transactionIndex = tdf['transactionIndex'], 
                        logIndex = tdf['logIndex'], 
                        transactionHash = tdf['transactionHash'],
                        tokenId = tdf['tokenId'])
            
            if not self.positions.empty:
                pos_dfs.append(self.positions)

        position_df = pd.concat(pos_dfs) #can save for different profit in state
        position_df.drop_duplicates(subset=['last_L', 'start_L', 'tickLower', 'tickUpper', 'owner',
                                            'start_token0_holdings', 'start_token1_holdings',
                                            'last_token0_holdings', 'last_token1_holdings', 'token0_fees_accrued',
                                            'token1_fees_accrued', 'token0_collected', 'token1_collected', 'start_logIndex', 
                                            'start_blockNumber', 'start_transactionIndex', 'start_transactionHash',], keep = 'first', inplace = True)
        return position_df
    
    def get_liquidity_distribution(self):
        pass

    def quote_price(self):
        pass


class TickPriceAllignmentError(Exception):
    """Raise when tick and sqrtPrice do not allign"""

class IncorrectInput(Exception):
    """Raise when input is incorrect"""

class SwapAllignmentError(Exception):
    """Raise when swap data is misalligned"""

class BurnMintMatchError(Exception):
    """Raise when a burn event cant be matched with a previous mint"""

class CollectMatchError(Exception):
    """Raise when a Collect event cant be matched with a active position"""

class SwapAmountError(Exception):
    """Raise when swap has two positive amounts"""

class FeeMismatch(Exception):
    """Raise when swap fee collected and distributed do not match"""