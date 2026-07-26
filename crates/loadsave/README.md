# nectar-loadsave

Manifest node persistence over the nectar file pipeline.

[`NodeLoadSaver`] adapts a chunk store to the mantaray persistence seam:
loads join a node's chunks through the file reader, saves split node bytes
through the file splitter, so a node larger than one chunk is addressed by
its file root, matching the reference client.

## Licence

AGPL-3.0-or-later.
