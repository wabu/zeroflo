from contextlib import contextmanager
import os
import asyncio

import logging
logger = logging.getLogger(__name__)

fork_debug = True

def fork_debugger(namespace=None):
    logger.warn('forking with ipython kernel for debugging ...')
    #pid = os.fork()
    pid = 0
    if not pid:
        try:
            from IPython import embed_kernel, get_ipython, Config, config, terminal
            from IPython.kernel.zmq import kernelapp
            curr = get_ipython()
            if curr:
                logger.warn("there's a current ipython running (%r)", curr)
                for cls in terminal.interactiveshell.TerminalInteractiveShell.mro():
                    if hasattr(cls, 'clear_instance'):
                        cls.clear_instance()
                for cls in kernelapp.IPKernelApp.mro():
                    if hasattr(cls, 'clear_instance'):
                        cls.clear_instance()

            conf = Config()
            conf.InteractiveShellApp.code_to_run = 'raise'
            if namespace:
                conf.IPKernelApp.connection_file = '{}/kernel.json'.format(namespace)
            logger.warn('starting ipython for debugging (%s)', namespace)
            embed_kernel(config=conf)
            logger.warn('embeding finished with debugging (%s)', namespace)
        except Exception as e:
            logger.error('failed to embed ipython: %s', e, exc_info=True)
        finally:
            #os._exit(1)
            pass
    else:
        logger.warn('forked %d for debugging', pid)

@contextmanager
def maybug(info=None, namespace=None):
    """
    forks a debugger when an execption is thrown
    """
    try:
        yield
    except Exception as e:
        logger.error('exception occured inside %s', info or 'maybug context', exc_info=True)
        if fork_debug:
            fork_debugger(namespace=namespace)
        raise
