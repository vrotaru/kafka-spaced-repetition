# Kafka Spaced Repetion Example

> Preconditions: You have Kafka runing on `localhost:9032` and Schema Registry running on `localhost:8081` and `just` installed on you computer. And Java (24)

## How to use

To add a question 

```Shell
just add # and follow the prompts
```
To review added questions 

```Shell
just review
```

## Motivation

I wanted to play with Kafka. And have logic related to data which is not just printing it out.

So it works like this. I have a programs which add questions (with answers) to a Kafka topic.
And other program which reads a batch of question form this topic and checks the answers,
and writes the question with the updated number of corect answers back to the same topic.

It is not even a toy. And it obviously undersells both spaced repetion systems and Kafka.


## Some stuff which I may add.

  - [x] A shutdown hook to write back the unreviewed questions.
  - [x] An Avro serializer, instead of the custom one.
