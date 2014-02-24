/*********************************************************************
 *
 * Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
 *
 * This file is provided to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License.  You may obtain
 * a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 *********************************************************************/

#include <riak.h>

/*
gcc `pkg-config --cflags --libs riak-c-client` taste_of_riak.c
*/

int
main(int argc, char *argv[])
{
    // a riak_config serves as your per-thread state to interact with Riak
    riak_config *cfg;

    // use the default configuration
    riak_error err = riak_config_new_default(&cfg);

    riak_object *obj;
    riak_get_options *get_options;
    riak_delete_options *delete_options;

    char output[10240];

    // create some sample binary values to use
    riak_binary *bucket_bin   = riak_binary_copy_from_string(cfg, "TestBucket");
    riak_binary *key_bin      = riak_binary_copy_from_string(cfg, "TestKey");
    riak_binary *value_bin    = riak_binary_copy_from_string(cfg, "{\"ExampleData\":12345}");
    riak_binary *content_type = riak_binary_copy_from_string(cfg, "application/json");

    riak_connection  *cxn   = NULL;
    riak_operation   *rop   = NULL;

    // Create a connection with the default address resolver
    err = riak_connection_new(cfg, &cxn, "localhost", "10017", NULL);
    if (err) {
        fprintf(stderr, "Cannot connect to Riak on localhost:10017\n");
        exit(1);
    }

    err = riak_operation_new(cxn, &rop, NULL, NULL, NULL);

    get_options = riak_get_options_new(cfg);
    if (get_options == NULL) {
    //    riak_log_critical(cxn, "%s","Could not allocate a Riak Get Options");
        return 1;
    }

    riak_get_options_set_basic_quorum(get_options, RIAK_TRUE);
    riak_get_options_set_r(get_options, 2);

    riak_get_response *get_response = NULL;
    err = riak_get(cxn, bucket_bin, key_bin, get_options, &get_response);
    if (err == ERIAK_OK) {
        riak_print_get_response(get_response, output, sizeof(output));
        printf("%s\n", output);
    }
    riak_get_response_free(cfg, &get_response);

    riak_get_options_free(cfg, &get_options);
    if (err) {
        fprintf(stderr, "Get Problems [%s]\n", riak_strerror(err));
        exit(1);
    }

// PUT
/*
            obj = riak_object_new(cfg);
            if (obj == NULL) {
                riak_log_critical(cxn, "%s","Could not allocate a Riak Object");
                return 1;
            }
            riak_object_set_bucket(cfg, obj, riak_binary_copy_from_string(cfg, args.bucket));
            if (args.has_key) {
                riak_object_set_key(cfg, obj, riak_binary_copy_from_string(cfg, args.key));
            }
            riak_object_set_value(cfg, obj, riak_binary_copy_from_string(cfg, args.value));
            if (riak_object_get_bucket(obj) == NULL ||
                riak_object_get_value(obj) == NULL) {
                fprintf(stderr, "Could not allocate bucket/value\n");
                riak_free(cfg, &obj);
                exit(1);
            }
            riak_put_options *put_options = riak_put_options_new(cfg);
            if (put_options == NULL) {
                riak_log_critical(cxn, "%s","Could not allocate a Riak Put Options");
                return 1;
            }
            riak_put_options_set_return_head(put_options, RIAK_TRUE);
            riak_put_options_set_return_body(put_options, RIAK_TRUE);

            if (args.async) {
                err = riak_async_register_put(rop, obj, put_options, (riak_response_callback)example_put_cb);
            } else {
                riak_put_response *put_response = NULL;
                err = riak_put(cxn, obj, put_options, &put_response);
                if (err == ERIAK_OK) {
                    riak_put_response_print(put_response, output, sizeof(output));
                    printf("%s\n", output);
                }
                riak_put_response_free(cfg, &put_response);
            }
            riak_object_free(cfg, &obj);
            riak_put_options_free(cfg, &put_options);
            if (err) {
                fprintf(stderr, "Put Problems [%s]\n", riak_strerror(err));
                exit(1);
            }
            break;



// DELETE
            delete_options = riak_delete_options_new(cfg);
            if (delete_options == NULL) {
                riak_log_critical(cxn, "%s","Could not allocate a Riak Delete Options");
                return 1;
            }
            riak_delete_options_set_w(delete_options, 1);
            riak_delete_options_set_dw(delete_options, 1);
           if (args.async) {
                err = riak_async_register_delete(rop, bucket_bin, key_bin, delete_options, (riak_response_callback)example_delete_cb);
            } else {
                err = riak_delete(cxn, bucket_bin, key_bin, delete_options);
            }
            riak_delete_options_free(cfg, &delete_options);
            if (err) {
                fprintf(stderr, "Delete Problems [%s]\n", riak_strerror(err));
                exit(1);
            }

    if (args.async) {
        // Terminates only on error or timeout
        event_base_dispatch(base);
    }

    // cleanup
    event_base_free(base);
    riak_free(cfg, &bucket_bin);
    riak_free(cfg, &key_bin);
    riak_free(cfg, &value_bin);
    riak_free(cfg, &index_bin);
    riak_free(cfg, &content_type);
    riak_config_free(&cfg);
*/
    return 0;
}

