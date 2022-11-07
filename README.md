# slkspec

This is work in progress!

```python
import fsspec

with fsspec.open("slk://arch/project/user/file", "r") as f:
    print(f.read())
```
