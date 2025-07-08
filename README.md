# Kafka & Data Products Assessment

## System

We have a kafka system with two input topics "flights" and "weather". The flights topic will contain details of every 
flight as it lands at heathrow. The weather topic contains the live weather reports from the runway.

The weather topic is a real time and strictly ordered feed of a high quantity of data, with a reliable update every five
seconds. The flights feed is still strictly orderd, will never get ahead of the weather feed, and messages will be 
infrequent, and arrive a minimum of five minutes after landing, and typically within 10 minutes (although this is not a 
guarantee).

So if a 10:00 AM message is in flights, it is safe to assume the 10:00AM message is already in the weather stream.

All of the data will be json, with one json record per message, this has already been validated and cleaned upstream
of this process. Formal schemas are provided, however, some examples are provided here:

For the flights topic:
```json
{
  "landing": {
    "flight": "BA551",
    "callsign":"BAW551",
    "model": "Airbus A320-232",
    "tail": "G-EUUY",
    "touchdown_time": 1738228685
  }
}
```

For the weather topic:
```json
{
  "ts": "2025-01-30T09:18:05Z",
  "report": "3-17kt@311, v24km, 3C, clear"
}
```

## Requirements

We need to bring to rest the flights data in some format, with the latest weather report at the time of landing attached
as an additional field. In the above case, as the `touchdown_time` is an exact match to `ts`, we will attach these 
records.

Note that the timestamps are in linux style in the flights source, but iso style in the weather source.

The exact method of output is not specified, as long as there is a method to retrieve it at a later point in time.

Clear instructions on how to run your solution would be appreciated.

Please submit your response either in a private git repo, adding [Lucy Martin](mailto:lucy.martin@nttdata.com) and 
[James Braun](mailto:james.braun@nttdata.com) as invites, or by committing your work to git locally, zipping up the 
whole folder (including the .git directory) and mailing it to us.

## Provided

We have provided the following things to help start this project, feel free to use them or not

### Schemas

In the schema folder are the json schemas that all data will be in.

### Samples

In the samples folder, there are sample input files for both types (In reality we dont have two flights land 10 seconds
apart, this is fake data for convenience).

### Setup

This folder contains a simple docker compose file to help running kafka locally if this is needed, and a python script
that will load the content of samples into kafka. It is acceptable to use this as is, or to edit it as needed.

### Outline

This is a starting point for the application, which you could choose to use. It sets up a consumer for the flights 
topic, but does not do any processing. Again, you can directly use, modify or ignore this. 

## Expectations

We do not expect an enterprise grade solution, but a basic implementation in python is expected, which we will review 
before and in interview, with our focus being on:

1) Readability of the code
2) Technical accuracy and edge cases
3) Any testing (either from write ups, or unit tests)
4) Any changes to, or notes about, the kafka configuration
5) Any notes, thoughts or TODOs about how this might be done if you had more time and a team to perform this
6) Basic use of git

We will not be assessing on:

* The exact output format used
* Any handling of rainy day scenarios
* Any choices made with respect to libraries or tools
* The time taken on the task

If there is not time to complete the task, we will review whatever is provided, but please attempt to work towards a
solution.

## Questions

If you have any questions, please send an email to [Lucy Martin](mailto:lucy.martin@nttdata.com) and 
[James Braun](mailto:james.braun@nttdata.com), and cc in any other individuals involved in your recruitment.