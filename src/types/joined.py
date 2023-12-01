raise NotImplementedError()

@vv.store_var
class Joined:
    _joined_name: str
    _joined_data: Dict[str, Any]# = None
    def __init__(self, name:str, data: Dict[str, Any]):
        self.__dict__['_joined_name'] = name
        self.__dict__['_joined_data'] = vv.dict(name)

    def apply(self, fn, *args, **kwargs):
        dd = vv.dict(f"{self._joined_name}_apply")
        for id, val in self._joined_data.items():
            dd[id] = val.__getkey__(key)

        return self.__class__(dd)

    def __getkey__(self, key):
        dd = vv.dict(f"{self._joined_name}_get-{key}")
        for id, val in self._joined_data.items():
            dd[id] = val.__getkey__(key)

        return self.__class__(dd)

    def __repr__(self):
        return f"<Joined _joined_data={ {key: '...' for key in self._joined_data.keys()} }>"
    
    def __str__(self):
        return f"<Joined _joined_data={ {key: '...' for key in self._joined_data.keys()} }>"

    def __iter__(self):
        for val in self._joined_data.values():
            for v in val:
                yield v

    def __len__(self):
        return sum(map(len, self._joined_data.values()))

    def __getattr__(self, key):
        dd = vv.dict(f"{self._joined_name}_geta-{key}")
        for id, val in self._joined_data.items():
            dd[id] = getattr(val, key)

        return self.__class__(dd)

    def __setattr__(self, key, v):
        for val in self._joined_data.values():
            setattr(val, key, v)

    def __call__(self, *args, **kwargs):
        dd = vv.dict(f"{self._joined_name}_call-{len(args)}-{len(kwargs)}")
        for id, val in self._joined_data.items():
            dd[id] = val(*args, **kwargs)

        return self.__class__(dd)
        
