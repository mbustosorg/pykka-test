import pykka
from typing import Dict

GetMessages = object()


class Price(pykka.ThreadingActor):
    def __init__(self, variables: Dict[str, pykka.ThreadingActor]):
        super().__init__()
        self._source_data: pykka.ThreadingActor = variables['source_data']
        self._persistence = variables['persistence']
        self._computed_values = {}

    def on_receive(self, message):
        if message in self._computed_values:
            return self._computed_values[message]
        price = self._source_data.ask(('price', message))
        self._computed_values[message] = price
        self._persistence.tell(('price', message, price))
        return self._computed_values[message]


class Book(pykka.ThreadingActor):
    def __init__(self, variables: Dict[str, pykka.ThreadingActor]):
        super().__init__()
        self._source_data: pykka.ThreadingActor = variables['source_data']
        self._persistence = variables['persistence']
        self._computed_values = {}

    def on_receive(self, message):
        if message in self._computed_values:
            return self._computed_values[message]
        book = self._source_data.ask(('book', message))
        self._computed_values[message] = book
        self._persistence.tell(('book', message, book))
        return self._computed_values[message]


class Assets(pykka.ThreadingActor):
    def __init__(self, variables: Dict[str, pykka.ThreadingActor]):
        super().__init__()
        self._source_data: pykka.ThreadingActor = variables['source_data']
        self._persistence = variables['persistence']
        self._computed_values = {}

    def on_receive(self, message):
        if message in self._computed_values:
            return self._computed_values[message]
        asset = self._source_data.ask(('assets', message))
        self._computed_values[message] = asset
        self._persistence.tell(('assets', message, asset))
        return self._computed_values[message]


class BookToPrice(pykka.ThreadingActor):
    def __init__(self, variables: Dict[str, pykka.ThreadingActor]):
        super().__init__()
        self._price: pykka.ThreadingActor = variables['price']
        self._book: pykka.ThreadingActor = variables['book']
        self._persistence = variables['persistence']
        self._computed_values = {}

    def on_receive(self, message):
        if message in self._computed_values:
            return self._computed_values[message]
        price = self._price.ask(message)
        book = self._book.ask(message)

        self._computed_values[message] = book / price
        self._persistence.tell(('btop', message, self._computed_values[message]))

        return self._computed_values[message]


class AssetToPrice(pykka.ThreadingActor):
    def __init__(self, variables: Dict[str, pykka.ThreadingActor]):
        super().__init__()
        self._price: pykka.ThreadingActor = variables['price']
        self._asset: pykka.ThreadingActor = variables['asset']
        self._persistence = variables['persistence']
        self._computed_values = {}

    def on_receive(self, message):
        if message in self._computed_values:
            return self._computed_values[message]
        price = self._price.ask(message)
        asset = self._asset.ask(message)

        self._computed_values[message] = asset / price
        self._persistence.tell(('atop', message, self._computed_values[message]))

        return self._computed_values[message]


class SourceData(pykka.ThreadingActor):
    def __init__(self, variables):
        super().__init__()
        self._variables = variables

    def on_receive(self, message):
        if message[0] == 'price':
            return 100.0
        elif message[0] == 'assets':
            return 100.0
        elif message[0] == 'book':
            return 100.0


class Persistance(pykka.ThreadingActor):
    def __init__(self, variables):
        super().__init__()
        self._variables = variables

    def on_receive(self, message):
        print(message)


if __name__ == '__main__':
    variables = {}
    variables['persistence'] = Persistance.start(variables)
    variables['source_data'] = SourceData.start(variables)
    variables['price'] = Price.start(variables)
    variables['book'] = Book.start(variables)
    variables['asset'] = Assets.start(variables)
    variables['btop'] = BookToPrice.start(variables)
    variables['atop'] = AssetToPrice.start(variables)
    future = variables['atop'].ask(200101, block=False).join(variables['btop'].ask(200101, block=False))
    print(future.get())
