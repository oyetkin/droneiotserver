import yaml
from typing import Dict

def load_yaml(source: str) -> Dict:
	fs = open(source, "r")

	data: Dict = yaml.load(fs, Loader=yaml.SafeLoader)
	fs.close()
	return data