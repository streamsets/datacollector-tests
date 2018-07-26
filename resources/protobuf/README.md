# How to generate protobuf .desc file
If by some reason you need to modify the protobuf objects schema, you will need a new .desc file, you can do so by modifying whatever you need and re-building it.

You can found all the information in [Confluence](https://streamsets.com/documentation/datacollector/latest/help/datacollector/UserGuide/Data_Formats/Protobuf-Prerequisites.html)

Or just running:
```
protoc --include_imports --descriptor_set_out=addressbook.desc addressbook.proto
```