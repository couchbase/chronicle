/**
 * @author Couchbase <info@couchbase.com>
 * @copyright 2020 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
**/

#include <stdbool.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "erl_driver.h"
#include "erl_nif.h"

static ERL_NIF_TERM am_ok;
static ERL_NIF_TERM am_error;
static ERL_NIF_TERM am_close_failed;


static ERL_NIF_TERM
make_errno_atom(ErlNifEnv *env, int errnum)
{
    return enif_make_atom(env, erl_errno_id(errnum));
}

static ERL_NIF_TERM
make_errno_error(ErlNifEnv *env, int errnum)
{
    return enif_make_tuple2(env,
                            am_error,
                            make_errno_atom(env, errnum));
}

ERL_NIF_TERM
do_sync_dir(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary path;

    if (!enif_inspect_binary(env, argv[0], &path)) {
        return enif_make_badarg(env);
    }

    int fd;

    while (true) {
        fd = open((char *) path.data, O_RDONLY | O_DIRECTORY);
        if (fd >= 0) {
            break;
        }

        if (errno != EINTR) {
            return make_errno_error(env, errno);
        }
    }

    int sync_ret = fsync(fd);
    int sync_errno = errno;

    if (close(fd) != 0) {
        ERL_NIF_TERM reason = enif_make_tuple2(env,
                                               am_close_failed,
                                               make_errno_atom(env, errno));

        return enif_raise_exception(env, reason);
    }

    if (sync_ret == 0) {
        return am_ok;
    } else {
        return make_errno_error(env, sync_errno);
    }
}

static ErlNifFunc nif_functions[] = {
    {"do_sync_dir", 1, do_sync_dir, ERL_NIF_DIRTY_JOB_IO_BOUND},
};

static int
load(ErlNifEnv *env, void** priv_data, ERL_NIF_TERM load_info)
{
    am_ok = enif_make_atom(env, "ok");
    am_error = enif_make_atom(env, "error");
    am_close_failed = enif_make_atom(env, "close_failed");

    return 0;
}

ERL_NIF_INIT(chronicle_utils, nif_functions, load, NULL, NULL, NULL);
