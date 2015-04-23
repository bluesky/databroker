
import logging
logger = logging.getLogger(__name__)

handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

py3_errmsg = ('Failed to import enaml, but this is expected on python 3.'
              'Eating exception and continuing.')
