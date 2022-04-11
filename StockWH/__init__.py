
__all__ = ['Stock', 'FutOpt']

def update():
    from StockWH import Stock, FutOpt
    Stock.update()
    FutOpt.update()

def backup():
    from StockWH import Stock, FutOpt
    Stock.backup()
    FutOpt.backup()
