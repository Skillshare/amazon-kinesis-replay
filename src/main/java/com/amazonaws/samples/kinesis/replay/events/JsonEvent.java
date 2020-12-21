/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.amazonaws.samples.kinesis.replay.events;

import com.amazonaws.util.json.Jackson;
import com.fasterxml.jackson.databind.JsonNode;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Comparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonEvent extends Event {
  private static final Logger LOG = LoggerFactory.getLogger(JsonEvent.class);
  public final Instant timestamp;
  public final Instant ingestionTime;

  public JsonEvent(String payload, Instant timestamp, Instant ingestionTime) {
    super(payload);

    this.timestamp = timestamp;
    this.ingestionTime = ingestionTime;
  }

  public static final Comparator<JsonEvent> timestampComparator =
      (JsonEvent o1, JsonEvent o2) -> o1.timestamp.compareTo(o2.timestamp);

  public static final Comparator<JsonEvent> ingestionTimeComparator =
      (JsonEvent o1, JsonEvent o2) -> o1.ingestionTime.compareTo(o2.ingestionTime);

  public static class Parser {
    private Instant ingestionStartTime = Instant.now();
    private Instant firstEventTimestamp;
    private Instant previousEventTimestamp = Instant.now();

    private final float speedupFactor;
    private final String timestampAttributeName;

    public Parser(float speedupFactor, String timestampAttributeName) {
      this.speedupFactor = speedupFactor;
      this.timestampAttributeName = timestampAttributeName;
    }

    public JsonEvent parse(String payload) {
      JsonNode json = Jackson.fromJsonString(payload, JsonNode.class);
      Instant timestamp;

      try {
        // JR: get "properties" then nested timestampAttributeName, assume sql Timestamp format
        String strTimestamp = json.get("properties").get(timestampAttributeName).asText();
        Timestamp sqlTimestamp = Timestamp.valueOf(strTimestamp);
        timestamp = sqlTimestamp.toInstant();
      } catch (NullPointerException e) {
        // JR: if event doesn't have timestamp, use previous event's timestamp for sorting
        timestamp = previousEventTimestamp;
      }

      if (firstEventTimestamp == null) {
        firstEventTimestamp = timestamp;
      }

      previousEventTimestamp = timestamp;

      long deltaToFirstTimestamp = Math.round(Duration.between(firstEventTimestamp, timestamp).toMillis()/speedupFactor);

      Instant ingestionTime = ingestionStartTime.plusMillis(deltaToFirstTimestamp);

      return new JsonEvent(payload, timestamp, ingestionTime);
    }

    public void reset() {
      firstEventTimestamp = null;
      ingestionStartTime = Instant.now();
    }
  }
}
