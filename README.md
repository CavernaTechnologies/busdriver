# bus-driver: Azure Service Bus driver for Go

The goal of bus-driver is to make Azure Service Bus easier to work with on goal. It is built on top of the Azure provided azservicebus library. 

bus-driver provides a few conveniences

- Consumer: struct for reading from a queue or topic
- Job: Message wrapper that helps manage lifetime
- Handlers: Declarative function format that uses the message subject as a route guide

bus-driver was created for use on my own projects, but I would love help in it's design and direction. Please open issues and PRs as needed and I will review them as I have time!