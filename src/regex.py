import re

decorators_re = re.compile(r"@.*?\.(launch|store_\w+|task).*?\n")