Major overhaul of commands.py
-----------------------------

 - removed BeamlineConfig and all of it's associated functions.
   This collection was never used and should be replaced by beam-line
   specific keys (which can be validated by bluesky).
 - rename `insert_event_descriptor` -> `insert_descriptor`
 - rename `find_event_descriptor` -> `find_descriptors`
 - raise custom events for NoRunStop and NoEventDescriptors
 - raise when trying to insert a second RunStop for a RunStart (but still
   has race condition)
 - Re-wrote Document, now read-only
 - removed `reorganize_event`
 - No longer accept ObjectIds as input to any public function
 - uid now obligatory to all insert functions
 - don't expose the ODM templates in api
 - documents now come out ascending in time, not descending.  It goes
   faster this way ?!?
 - key name with '.' are now simply forbidden
