import logging
import zeroflo as flo

from pyadds.logging import log


"""
Fibonacci Flow
--------------

it consists of two simple units:
- Add unit adds values from two diffrent inports
- Lag unit delays the output

A simple printer is used to show the results


Here's the flow diagram showing the wireing:
```
  ~ space {add} ~      | ~ space {print,lag} ~
                       |
                       |
   .----------------.  |
  :                _.` |
  `-> a][add][out =----|-> ins][print]
  .-> b]           `.  |   
  :                  `-|-> ins][delay][out -.
  :                    |                    :
  `--------------------|--------------------`
                       |
```
The distribute into different process spaces is arbitrary,
but shows how easy it es to setup distributed flows.
"""

@log
class Lag(flo.Unit):
    def __init__(self, *args, **kws):
        super().__init__(*args, **kws)
        self.setup = False

    @flo.outport
    def out(i, tag): pass

    @flo.inport
    def ins(self, i, tag):
        print('lag', i >> tag)
        if self.setup:
            self.__log.debug('>> %d << %d', self.last, i)
            yield from self.last >> tag.add(lag=1) >> self.out
        else:
            self.__log.debug('<< %d', i)
            self.setup = True

        self.last = i


@log
class Add(flo.Unit):
    @flo.outport
    def out(i, tag): pass

    @flo.combine
    def add(self, a, b, tag):
        yield from a + b >> tag >> self.out

    @flo.async
    @flo.inport
    def a(self, a, tag):
        self.__log.debug('<<a %s (%s)', a, tag.lag)
        yield from self.add(a=a, tag=tag)

    @flo.async
    @flo.inport
    def b(self, b, tag):
        self.__log.debug('<<b %s (%s)', b, tag.lag)
        yield from self.add(b=b, tag=tag)


@log
class Print(flo.Unit):
    @flo.inport
    def ins(self, o, tag):
        if o > 1e78:
            raise ValueError("raising to show how to debug")
        print('>>', o)

def setup_logging():
    logging.basicConfig(format='[%(process)d] %(levelname)5s:%(name) s%(message)s')
    logging.getLogger('zeroflo').setLevel("DEBUG")
    #logging.getLogger('examples').setLevel("DEBUG")
    #logging.getLogger('zeroflo.tools').setLevel("DEBUG")
    logging.getLogger('zeroflo.core.zmqtools').setLevel("INFO")


if __name__ == "__main__":
    from examples.fib import *

    ctx = flo.Context('fib')

    with ctx.setup(setup=setup_logging):
        # create flow units
        add = Add()
        lag = Lag()
        prt = Print()

        # connect flow units
        add.out >> add.a
        add.out >> lag.ins
        lag.out >> add.b

        add.out >> prt.ins

        # specifiy distribution
        add & prt | lag

    with ctx.run():
        # simple call to trigger flow
        add.a(0)
        add.b(0)
        print('--')
        add.b(1)
        prt.join()

