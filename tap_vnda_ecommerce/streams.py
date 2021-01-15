from datetime import timedelta, datetime
import singer

LOGGER = singer.get_logger()
BOOKMARK_DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

class Stream:
    def __init__(self, client):
        self.client = client


class OrderItems(Stream):
    tap_stream_id = 'order_items'
    key_properties = ['id']
    replication_method = 'INCREMENTAL'
    valid_replication_keys = ['updated_at']
    replication_key = 'updated_at'

    def sync(self, state, stream_schema, stream_metadata, config, transformer):
        start_time_str = singer.get_bookmark(state, self.tap_stream_id, self.replication_key, config['start_date'])
        start_time = datetime.strptime(start_time_str,BOOKMARK_DATE_FORMAT).astimezone()
        
        # TODO: remove:
        LOGGER.info(type(start_time))
        LOGGER.info(start_time)

        max_record_value = start_time
        
        # building the request parameters
        start_date = datetime.strftime(start_time,'%Y%m%d')
        finish_date = datetime.strftime(singer.utils.now(),'%Y%m%d')
        
        
        # get orders
        for record in self.client.get_orders(start_date,finish_date):
            # as bookmark is truncated at 'day level' do requests, ignore records older than last bookmark timestamp.
            if record[self.replication_key] >= start_time:
                transformed_record = transformer.transform(record, stream_schema, stream_metadata)
            
                singer.write_record(self.tap_stream_id,transformed_record)

                if record[self.replication_key] > max_record_value:
                    max_record_value = transformed_record[self.replication_key]

        state = singer.write_bookmark(state, self.tap_stream_id, self.replication_key, max_record_value)
        singer.write_state(state)
        
        return state


STREAMS = {
    'order_items': OrderItems
}