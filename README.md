# Nifi Custom ListenTCP Processor

This project is based on a fork of the following repository: https://github.com/linksmart/nifi-customlistentcp-processor.git

The changes applied by NorCom to the original code are available on GitHub: https://github.com/dasense/nifi-customlistentcp-processor

This is a custom processor based on the default `ListenTCP` processor provided by Nifi. In the default `ListenTCP` processor, the delimiter for separating incoming message is hard-coded as `\n` (Note that there is a similar option in the default ListenTCP called `Batching Message Delimiter`, which is the delimiter for concatenating a batch of outgoing messages). 
The underlying modification of the default ListenTCP processor does not expect to receive messages delimited by a character. Instead, length-delimited messages are expected. 
This means that the processor expects a 4 byte length information, which precedes every message.

## Build Instructions
Build it with Maven:
```
mvn clean install
```
Find the built nar file here:
```
<repo directory>/nifi-custom-listen-tcp-nar/target/nifi-custom-listen-tcp-nar-<version number>.nar
```
and copy it to the following directory of the running Nifi instance:
```
/opt/nifi/nifi-1.4.0/lib
```
Restart Nifi, then you can find the new ``CustomListenTCP`` processor available. You can specify if the length information should be kept in the property field `Keep incoming message length information`.