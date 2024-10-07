from __future__ import annotations

import hashlib
import io
import logging
import os
import pathlib
import re
import traceback
from typing import BinaryIO, List

import requests

from .errors import S3UploadError


logger = logging.getLogger(__name__)

MiB = 1024 ** 2
GiB = 1024 ** 3


class FilePart(io.IOBase):
    def __init__(self,
                 file_object: io.FileIO | BinaryIO,
                 part_number: int,
                 part_size: int,
                 file_size: int = None,
                 min_read_size: int = 4 * MiB,
                 callback: callable = None,
                 ):
        """MD5-creating progress wrapper of file objects for partial access

        Uploading large files to S3 is normally via a multipart upload.
        This is not the multipart upload known from the `requests`
        package, but an S3-specific multipart upload. The file is split
        into multiple parts and each of the parts is uploaded individually.
        Partial access to a file is what this class facilitates. It also
        comes with the convenience of computing the MD5 sum (via the `md5`
        method) and a `callback` method for tracking the progress of the
        upload in another thread.

        Parameters
        ----------
        file_object: file-like
            object implementing seek and read
        file_size: int
            size of the file in bytes
        part_number: int
            multipart upload part number, indexing starts at 0
        part_size: int
            part size of each multipart upload part, the final
            part may be smaller than this number
        min_read_size: int
            minimum number of bytes to read for every call to the function;
            Note that this is a hack that speeds up the upload with
            Python requests, similar to what is proposed here:
            https://github.com/requests/toolbelt/issues/75
            #issuecomment-237189952
        callback: callable
            method to call for monitoring the progress; this is
            called when `read` is called and calls the method
            with the argument `amount_seen / file_size` (overall
            percentage, not only for the current part).

        Notes
        -----
        Do not use the same `file_object` for parallel uploads.
        The init method calls `file_object.seek` to go to the
        correct place in the file. If you want to do parallel
        uploads, you have to instantiate one file object for
        each `FilePart`.
        """
        self._hasher = hashlib.md5()
        self._md5 = None
        self.file_object = file_object
        self.part_number = part_number
        self.file_size = file_size
        self.part_offset = part_number * part_size
        self.min_read_size = min_read_size
        self.callback = callback

        if file_size is None:
            # Seek to the end to obtain the file size
            file_object.seek(0, os.SEEK_END)
            file_size = file_object.tell()

        # seek to the correct position
        self.file_object.seek(self.part_offset)

        # determine the correct size of the upload
        if self.part_offset + part_size > file_size:
            self.part_size = file_size - self.part_offset
        else:
            self.part_size = part_size

    def __iter__(self):
        """Iterate over the part in 4 MiB chunks"""
        self.seek(0)
        while dat := self.read(4 * MiB):
            yield dat

    @property
    def length(self):
        """Return the size of the part in bytes"""
        return self.part_size

    def md5(self):
        """Return the MD5 sum of the part"""
        if self._md5 is None:
            # This will compute the MD5 sum
            for _ in iter(self):
                pass
        return self._md5

    def read(self, size=-1, /):
        """Read up to `size` bytes from the part"""
        amount = size
        # Read large chunks for uploading data. This is a workaround
        # to uploading in 8k chunks, similar to the one discussed in
        # https://github.com/requests/toolbelt/issues/75#issuecomment-237189952
        if 1 < amount < self.min_read_size:
            amount = self.min_read_size
        cur_pos = self.tell()
        assert cur_pos <= self.part_size
        if cur_pos == self.part_size:
            # everything uploaded successfully
            return b""
        else:
            # Make sure we do not read more than we are allowed to
            if cur_pos + amount > self.part_size:
                amount = self.part_size - cur_pos
            data = self.file_object.read(amount)
            if self._md5 is None:
                self._hasher.update(data)
            if cur_pos + amount == self.part_size:
                self._md5 = self._hasher.hexdigest()
            if self.callback:
                self.callback(
                    (self.part_offset + cur_pos) / self.file_size)
            return data

    def seek(self, offset, whence=os.SEEK_SET):
        """Seek to a position within the part

        Internally, this seeks through `self.file_object`.
        """
        # reset hasher
        if self._md5 is None:
            self._hasher = hashlib.md5()
        # perform actual seek
        if whence == os.SEEK_SET:
            self.file_object.seek(self.part_offset + offset)
        elif whence == os.SEEK_CUR:
            self.file_object.seek(offset, whence)
        elif whence == os.SEEK_END:
            self.file_object.seek(self.part_offset + self.part_size)

    def tell(self):
        """Return the current position in the part

        This is the position in the original `file_object` minus
        the part offset.
        """
        return self.file_object.tell() - self.part_offset


class UploadMonitorLink:
    def __init__(self, fd, file_size, monitor_callback):
        """Monitor upload by checking the current seek position of a file"""
        self.fd = fd
        self.len = file_size
        self.monitor_callback = monitor_callback
        self.bytes_read = 0

    def callback(self, *args, **kwargs):
        self.bytes_read = self.fd.tell()
        if self.monitor_callback is not None:
            return self.monitor_callback(self)


def assemble_complete_multipart_xml(parts_etags):
    """Create XML data for completing a multipart upload

    When all parts of a multipart upload have been uploaded,
    you have to tell S3 that now everything is done such that
    it can concatenate the parts into one big file. For this,
    we have to tell S3 again the ETags of the individual parts
    in the correct order. This is done in XML. This method
    produces this XML data given a list of correctly ordered ETags.
    """
    s = "<CompleteMultipartUpload>\n"
    for ii, etag in enumerate(parts_etags):
        s += ("  <Part>\n"
              + f"    <PartNumber>{ii + 1}</PartNumber>\n"
              + f"    <ETag>{etag}</ETag>\n"
              + "  </Part>\n"
              )
    s += "</CompleteMultipartUpload>"
    return s


def compute_upload_part_parameters(file_size):
    """Given a file of certain size, return sizes and number of parts

    Parameters
    ----------
    file_size: int
        file size in bytes

    Returns
    -------
    parms: dict
        dictionary with the keys:

        - "num_parts": number of parts of the upload
        - "part_size": size of the parts (except for the last part)
        - "part_size_last": size of the last part
        - "file_size": same as input parameter
    """
    gib = 1024**3

    # Compute number of parts
    if file_size % gib == 0:
        num_parts = file_size // gib
    else:
        num_parts = file_size // gib + 1

    # Compute part file size
    if file_size % num_parts == 0:
        # Every part has the same size, since the file size is
        # a multiple of the number of parts.
        part_size = file_size // num_parts
    else:
        # The last part is a few bytes smaller than the other parts.
        part_size = file_size // num_parts + 1

    part_size_last = file_size - part_size * (num_parts - 1)

    return {
        "num_parts": num_parts,
        "part_size": part_size,
        "part_size_last": part_size_last,
        "file_size": file_size,
    }


def get_etag_from_response(response):
    """Given a response from a PUT or POST request, extract the ETag

    Depending on the software running the object store, the server
    may return the ETag in different ways. This is a helper method
    that properly extracts the ETag.
    """
    etag = None

    # Get the ETag from the response header
    for etag_key in [
        "etag",  # OpenStack / Ceph(Quincy), only for single PUT requests
        "ETag",  # minio (this is actually the correct name)
        "Etag",  # other?
        "ETAG",  # who knows...
    ]:
        etag_str = response.headers.get(etag_key)
        if etag_str:
            etag = etag_str.strip("'").strip('"')
            break

    # Get the ETag from the request body
    if etag is None:
        xml_etag_regexp = re.compile("<ETag>([a-f0-9]*)</ETag>", re.IGNORECASE)
        body = response.content.decode("utf-8")
        xml_search = xml_etag_regexp.findall(body)
        if len(xml_search) == 1:  # If it is more than one, could be the parts
            etag = xml_search[0]
    return etag


def requests_put_data_and_get_etag(
        file_object: io.FileIO | BinaryIO,
        put_url: str,
        part_number: int,
        part_size: int,
        file_size: int,
        callback: callable,
        timeout=27.3,
        retries=3,
        ):
    """Upload data via a PUT request and return the ETag

    Parameters
    ----------
    part_number: int
        multipart upload part number, indexing starts at 0


    Since this method makes use of :class:`.FilePart` which
    supports on-the-fly MD5 sum computation, the ETag is
    automatically verified with the MD5 sum of the file.
    """
    fd_part = FilePart(file_object=file_object,
                       part_number=part_number,
                       part_size=part_size,
                       file_size=file_size,
                       callback=callback,
                       )
    for jj in range(retries):
        # Always seek to the zero position.
        fd_part.seek(0)
        trcbck = None
        resp = None
        try:
            logger.info(f"Uploading part {part_number + 1} with {put_url}")
            resp = requests.put(put_url,
                                data=fd_part,
                                timeout=timeout,
                                )
        except BaseException:
            trcbck = traceback.format_exc()
            logger.warning(f"Encountered Exception for {put_url}:\n{trcbck} ")
            continue
        else:
            # Obtain the ETag from the headers
            etag = get_etag_from_response(resp)
            if etag is not None and etag == fd_part.md5():
                break
    else:
        raise S3UploadError(
            f"Could not upload or server did not return 'ETag' for upload "
            f"of {fd_part}[{part_number}] using {put_url} "
            f"({retries} retries), got "
            f"{resp.headers if resp is not None else None} and {trcbck}")
    return etag


def upload_s3_presigned(
        path: pathlib.Path | str,
        upload_urls: List[str],
        complete_url: str = None,
        retries: int = 3,
        timeout: float = 27.3,
        callback: callable = None,
        ) -> str:
    """Upload data to an S3 bucket using presigned URLS

    For user convenience, this method performs some sanity checks
    such as minimum and maximum part size. In addition, the uploaded
    data are verified by checking the ETag returned by the object store.
    See also https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html.

    Parameters
    ----------
    path: str or pathlib.Path
        path to the file that is uploaded
    upload_urls: list of str
        list of presigned URLs (in the correct order) for the
        multipart upload
    complete_url: str
        URL for finishing the multipart upload
    retries: int
        How many times each request should be retried if failed
    timeout: float
        Timeout for requests
    callback: callable
        method to call for monitoring the progress; This is
        called periodically during the upload with a number
        between 0 (start) and 1 (finished).

    Returns
    -------
    etag: str
        ETag of the uploaded file in the object store
    """
    file_size = path.stat().st_size
    num_parts = len(upload_urls)

    parms = compute_upload_part_parameters(file_size)
    if num_parts != parms["num_parts"]:
        raise ValueError(f"Expected {parms['num_parts']} upload URLs, "
                         f"got {num_parts}")
    part_size = parms["part_size"]
    part_size_last = parms["part_size_last"]

    if part_size_last <= 0:
        # Something went wrong. If we attempt to upload the file
        # with the given number of parts, then there is no data
        # left (at least) for the final part.
        raise ValueError(
            f"We encountered an invalid upload scenario. You requested "
            f"an upload with too many ({num_parts}) parts for the file "
            f"'{path}' of size {file_size}. Either the requested number of "
            f"parts is too large, or the file is not what you think it is. "
            f"Note that for small files (<100MB), only one single part is "
            f"fine. For larger files, select the number of parts so that "
            f"it is above 100MB and below the S3 limit of 5GB.")

    if part_size > (5 * GiB):
        raise ValueError(
            f"The size for one upload part, given the file size of "
            f"{file_size / 1024 ** 3:.1f} GiB and the number of parts "
            f"{num_parts}, is above 5 GiB. This is the hard limit for "
            f"uploading a single part to the S3 object storage. Please "
            f"increase the number of parts for your upload.")

    if part_size_last < (5 * MiB) and num_parts > 1:
        raise ValueError(
            f"The size for one upload part, given the file size of "
            f"{file_size / 1024:.1f} kiB and the number of parts "
            f"{num_parts}, is below 5 MiB. Please set the number of "
            f"parts to one which will replace the multipart upload "
            f"with a single PUT request.")

    if num_parts == 1 and complete_url:
        raise ValueError(
            f"There is only one upload part, but {complete_url=} "
            f"is set. For non-multipart uploads, data should be uploaded via "
            f"a single PUT request and not via UploadPart.")

    if num_parts > 10000:
        raise ValueError(
            "There are too many upload URLs. For non-multipart uploads, the"
            "maximum number of parts is 10000.")

    # Perform the upload
    if num_parts == 1:
        # Upload with a single PUT request.
        etag = upload_s3_presigned_single(
            path=path,
            file_size=file_size,
            presigned_url=upload_urls[0],
            retries=retries,
            timeout=timeout,
            callback=callback,
            )
    else:
        # Multipart Upload
        etag = upload_s3_presigned_multipart(
            path=path,
            part_size=part_size,
            file_size=file_size,
            presigned_urls=upload_urls,
            complete_multipart_url=complete_url,
            retries=retries,
            timeout=timeout,
            callback=callback,
            )

    return etag


def upload_s3_presigned_multipart(
        path: pathlib.Path | str,
        part_size: int,
        file_size: int,
        presigned_urls: List[str],
        complete_multipart_url: str,
        retries: int = 3,
        timeout: float = 27.3,
        callback: callable = None,
        ) -> str:
    """Upload a dataset to S3 with multipart and a given upload ID

    The returned ETag is checked against the MD5 sum of the file.

    Parameters
    ----------
    path: str or pathlib.Path
        path to the file that is uploaded
    part_size: int
        size of each upload part in bytes
    file_size: int
        size of `path` in bytes
    presigned_urls: list of str
        list of presigned URLs (in the correct order) for the
        multipart upload
    complete_multipart_url: str
        URL for finishing the multipart upload
    retries: int
        How many times each request should be retried if failed
    timeout: float
        Timeout for requests
    callback: callable
        method to call for monitoring the progress; This is
        called periodically during the upload with a number
        between 0 (start) and 1 (finished).

    Returns
    -------
    etag: str
        ETag of the uploaded file in the object store. The ETag is
        computed during the upload in this function and then
        compared to the ETag returned via the S3 API. If the server
        does not return an ETag, then the expected ETag is still
        returned.

    Notes
    -----
    If you are interested in creating the presigned URLs used by this
    method, first take a look at the Amazon multipart upload docs:
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
    You have to create presigned URLs for the "put_object" method and for the
    "complete_multipart_upload" method. You can find an implementation
    in the DCOR code, method "create_presigned_upload_urls" in:
    https://github.com/DCOR-dev/dcor_shared/blob/master/dcor_shared/s3.py
    """
    retries = max(1, retries)
    path = pathlib.Path(path)
    # Everything is set for multipart upload. Commence with the multipart
    # upload.
    parts_etags = []
    with path.open("rb") as fd:
        monitor = UploadMonitorLink(fd=fd,
                                    file_size=file_size,
                                    monitor_callback=callback)
        for ii, psurl in enumerate(presigned_urls):
            etag_part = requests_put_data_and_get_etag(
                file_object=fd,
                put_url=psurl,
                part_number=ii,
                part_size=part_size,
                file_size=file_size,
                callback=monitor.callback,
                timeout=timeout,
                retries=retries,
            )
            parts_etags.append(etag_part)

    # Tell S3 to complete the upload and concatenate all the parts into
    # one file. Note that the response to `complete_multipart_upload` will
    # always be a 200 OK, even if there was an error. The response has to be
    # carefully inspected to make sure everything worked properly.

    # Compute resulting ETag
    hasher = hashlib.md5()
    for etag_part in parts_etags:
        etag_binary = int(etag_part, 16).to_bytes(length=16, byteorder="big")
        hasher.update(etag_binary)
    etag_expected = f"{hasher.hexdigest()}-{len(parts_etags)}"

    for jj in range(retries):
        resp_compl = None
        trcbck_compl = None
        try:
            resp_compl = requests.post(
                complete_multipart_url,
                data=assemble_complete_multipart_xml(parts_etags),
                timeout=timeout,
            )
        except BaseException:
            trcbck_compl = traceback.format_exc()
            logger.warning(
                f"Encountered {trcbck_compl} for {complete_multipart_url}")
            continue
        else:
            etag_full = get_etag_from_response(resp_compl)
            if etag_full:  # should not be None or an empty string
                if etag_full == etag_expected:
                    # This is the ideal case. Everything is good.
                    break
                else:
                    logger.warning(f"ETag mismatch, expected {etag_expected}, "
                                   f"got {etag_full}; (retry {ii})")
                    # The server returned the wrong ETag. We will try again.
                    continue
            else:
                # Some servers do not properly return the ETag in the response
                # (header), so we cannot rely on this being the case.
                # See e.g.  https://github.com/ceph/ceph/pull/51447
                # What we do instead is verify that there are no error messages
                # in the response body:
                rbody = resp_compl.content.decode("utf-8")
                if rbody.lower().count("<error>"):
                    logger.warning(
                        f"Server did not return ETag in the response header "
                        f"and returned an error message in the body: {rbody}"
                    )
                    continue
                else:
                    # We have to assume everything went well.
                    logger.info("Server did not return ETag in the response, "
                                "but there is no error message in the body; "
                                "I assume that the upload is complete.")
                    break
    else:
        err_msg = f"Not able to complete multipart upload ({retries} retries)"
        if resp_compl is not None:
            err_msg += (f"\nGot response {resp_compl.content} with header "
                        f"{resp_compl.headers}.")
        if trcbck_compl is not None:
            err_msg += f"\nGot traceback:\n{trcbck_compl}"

        raise S3UploadError(err_msg)

    return etag_expected


def upload_s3_presigned_single(
        path: pathlib.Path | str,
        file_size: int,
        presigned_url: str,
        retries: int = 3,
        timeout: float = 27.3,
        callback: callable = None
        ) -> str:
    """Upload a single file using a PUT request to a presigned URL

    The returned ETag is checked against the MD5 sum of the file.

    Parameters
    ----------
    path: str or pathlib.Path
        path to the file that is uploaded
    file_size: int
        size of `path`
    presigned_url: str
        presigned URL for the upload
    retries: int
        How many times each request should be retried if failed
    timeout: float
        Timeout for requests
    callback: callable
        method to call for monitoring the progress; This is
        called periodically during the upload with a number
        between 0 (start) and 1 (finished).

    Returns
    -------
    etag: str
        ETag of the uploaded file in object store

    Notes
    -----
    If you are interested in creating the presigned URL for a PUT request,
    take a look at the boto3 documentation here:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_object.html#put-object
    For an actual implementation in Python (which is used in DCOR), take a
    look at the method `create_presigned_upload_urls` here:
    https://github.com/DCOR-dev/dcor_shared/blob/master/dcor_shared/s3.py
    For a single PUT request, it looks like this::

        psurl = s3_client.generate_presigned_url(
             "put_object",
             Params={'Bucket': bucket_name,
                     'Key': object_name,
                     },
             ExpiresIn=expiration,
             HttpMethod='PUT',
         )
    """ # noqa
    retries = max(1, retries)
    with path.open("rb") as fd:
        monitor = UploadMonitorLink(fd=fd,
                                    file_size=file_size,
                                    monitor_callback=callback)
        etag = requests_put_data_and_get_etag(
            file_object=fd,
            put_url=presigned_url,
            part_number=0,
            part_size=file_size,
            file_size=file_size,
            callback=monitor.callback,
            timeout=timeout,
            retries=retries,
            )

    return etag
