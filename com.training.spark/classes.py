class BidError:
    def __init__(self, date, errorMessage):
        self.date = date
        self.errorMessage = errorMessage


class EnrichedItem:
    def __init__(self, motelId, motelName, bidDate, loSa, price):
        self.motelId = motelId
        self.motelName = motelName
        self.bidDate = bidDate
        self.loSa = loSa
        self.price = price

    def __str__(self):
        return ','.join([
            self.motelId,
            self.motelName,
            self.bidDate,
            self.loSa,
            str(self.price)])


class BidItem:
    def __init__(self, motelId, bidDate, loSa, price):
        self.motelId = motelId
        self.bidDate = bidDate
        self.loSa = loSa
        self.price = price
