# Event contracts

This directory is the temporary event schema registry described by
`TDD-foundation-platform-001`. Publishing systems own the event definitions; this
repository owns the compatibility gate while the enterprise registry is unavailable.

Register each event type once in `registry.json`, with its version list ordered from
oldest to newest. A new version may add optional properties. It may not remove an existing
property, change its type, or add a required property. Major breaking changes use a new
event type major and a separate registry entry.

```json
{
  "schemas": [
    {
      "event_type": "com.scnehaux.example.record.lifecycle.created",
      "major": 1,
      "versions": [
        "example/record.lifecycle.created/v1/1.0.0.json",
        "example/record.lifecycle.created/v1/1.1.0.json"
      ]
    }
  ]
}
```
