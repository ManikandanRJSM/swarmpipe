entity_mapper = {
    'dim' : {
        'assets',
        'sites',
        'buildings',
        'sensors',
    },

    'fact' : {
        'iot_telemetry',
        'events'
    }
}

topic_prefix = 'nectar_de.public'

merge_keys = {
    'assets': ['asset_id'],
    'sites': ['site_id'],
    'buildings': ['building_id'],
    'sensors': ['sensor_id'],
    'iot_telemetry': ['site_id', 'building_id', 'asset_id'],
    'events': ['asset_id'],
}


entity_schema = {
    'assets' : [
        'uuid', 
        'id', 
        'asset_id', 
        'asset_name', 
        'asset_type', 
        'manufacturer', 
        'installation_date', 
        'site_id', 
        'parent_asset_id', 
        'created_at'
    ],

    'sites' : [
        'uuid',
        'id',
        'site_id',
        'site_name',
        'address', 
        'city',
        'country',
        'timezone',
        'created_at'
    ],

    'buildings' : [
        'uuid', 
        'id',
        'building_id',
        'building_name',
        'site_id',
        'floor_count',
        'created_at',
    ],

    'sensors' : [
        'uuid',
        'id,'
        'sensor_id',
        'asset_id',
        'sensor_type',
        'unit',
        'installation_date',
        'created_at',
    ],

    'iot_telemetry' : [
        'uuid',
        'id',
        'timestamp',
        'site_id',
        'building_id',
        'asset_id',
        'sensor_id',
        'temperature',
        'humidity',
        'pressure',
        'vibration',
        'power_consumption',
        'operating_mode',
        'ingested_at',
    ],

    'events' : [
        'uuid',
        'id',
        'event_id',
        'timestamp',
        'asset_id',
        'event_type',
        'severity',
        'message',
        'ingested_at',
    ]
}