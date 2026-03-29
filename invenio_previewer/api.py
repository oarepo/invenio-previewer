# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""File reader utility."""

import zipfile
from os.path import basename, splitext

from flask import url_for


def create_preview_file(pid, record, fileobj, zip_path=None):
    """Factory function to create the appropriate PreviewFile instance.

    This factory handles creating either a standard PreviewFile or a
    ZipExtractedFile based on whether zip_path is provided.

    :param pid: Persistent identifier
    :param record: Record object
    :param fileobj: File object from record (ObjectVersion)
    :param zip_path: Optional path to file within ZIP archive
    :returns: PreviewFile or ZipExtractedFile instance
    :raises ValueError: If zip_path is provided but file is not a ZIP
    :raises FileNotFoundError: If file not found in ZIP archive
    """
    base_file = PreviewFile(pid, record, fileobj)
    if zip_path:
        # Verify the container file is a ZIP
        if not base_file.filename.lower().endswith(".zip"):
            raise ValueError("Container file must be a ZIP archive")

        # Return ZipExtractedFile for files within ZIP archives
        return ZipExtractedFile(pid, record, base_file, zip_path)
    return base_file


class PreviewFile(object):
    """Preview file default implementation."""

    def __init__(self, pid, record, fileobj):
        """Initialize object.

        :param file: ObjectVersion instance from Invenio-Files-REST.
        """
        self.file = fileobj
        self.pid = pid
        self.record = record

    @property
    def size(self):
        """Get file size."""
        return self.file["size"]

    @property
    def filename(self):
        """Get filename."""
        return basename(self.file.key)

    @property
    def bucket(self):
        """Get bucket."""
        return self.file.bucket_id

    @property
    def uri(self):
        """Get file download link.

        ..  note::

            The URI generation assumes that you can download the file using the
            view ``invenio_records_ui.<pid_type>_files``.
        """
        return url_for(
            ".{0}_files".format(self.pid.pid_type),
            pid_value=self.pid.pid_value,
            filename=self.file.key,
        )

    def is_local(self):
        """Check if file is local."""
        return True

    def has_extensions(self, *exts):
        """Check if file has one of the extensions."""
        file_ext = splitext(self.filename)[1].lower()
        return file_ext in exts

    def open(self):
        """Open the file."""
        return self.file.file.storage().open()


class ZipExtractedFile(PreviewFile):
    """Preview file extracted from within a ZIP archive.

    This class wraps a file from a ZIP archive and provides streaming access
    without loading the entire file into memory. Compatible with all existing preview extensions.
    At least I hope so :)

    """

    def __init__(self, pid, record, zip_fileobj, zip_path):
        """Initialize the ZIP extracted file.

        :param pid: Persistent identifier
        :param record: Record object
        :param zip_fileobj: The ZIP file object (PreviewFile or similar)
        :param zip_path: Path to the file within the ZIP archive
        """
        super().__init__(pid, record, zip_fileobj.file)
        self.zip_fileobj = zip_fileobj
        self.zip_path = zip_path
        self._file_info = None
        self._zipfile_ref = None
        self._zip_fp_ref = None

        # Load file metadata from cached listing or ZIP
        self._load_file_info()

    def _load_file_info(self):
        """Load file information from cached listing or ZIP archive."""
        # TODO: add info from listing file later to avoid opening ZIP every time

        try:
            with self.zip_fileobj.open() as zip_fp:
                with zipfile.ZipFile(zip_fp) as zf:
                    info = zf.getinfo(self.zip_path)
                    self._file_info = {
                        "size": info.file_size,
                        "compressed_size": info.compress_size,
                    }
        except KeyError:
            raise FileNotFoundError(f"File '{self.zip_path}' not found in ZIP archive")
        except Exception:
            # If we can't get info, use defaults
            self._file_info = {"size": 0}

    @property
    def size(self):
        """Return the uncompressed size of the file."""
        return self._file_info.get("size", 0)

    @property
    def filename(self):
        """Return the base name of the file (without path)."""
        return basename(self.zip_path)

    @property
    def uri(self):
        """Get file URI.

        For ZIP-extracted files, we return the file download URL with
        the zip_path parameter. This allows the file to be served directly
        (for <img> tags, PDFs, etc.) rather than rendering a preview page.
        """
        return url_for(
            ".{0}_files".format(self.pid.pid_type),
            pid_value=self.pid.pid_value,
            filename=self.file.key,
            zip_path=self.zip_path,
            _external=False,
        )

    def has_extensions(self, *exts):
        """Check if file has one of the extensions."""
        file_ext = splitext(self.zip_path)[1].lower()
        return file_ext in exts

    def open(self):
        """Open the file from within the ZIP archive.

        Returns a file-like object that streams data directly from the ZIP
        without extracting the entire file into memory.
        """
        # For streaming previews (images, PDFs, CSVs), we need to return
        # a wrapper that keeps the ZIP file open while the stream is being read
        return ZipFileStream(self)

    def is_local(self):
        """Check if file is local. I guess there are local."""
        return True


class ZipFileStream:
    """A file-like wrapper that streams data from within a ZIP archive."""

    def __init__(self, zip_extracted_file):
        """Initialize the stream wrapper.

        :param zip_extracted_file: ZipExtractedFile instance
        """
        self.zip_extracted_file = zip_extracted_file
        self._zip_fp = None
        self._zipfile = None
        self._stream = None
        self._opened = False

    def _ensure_open(self):
        """Lazily open the ZIP and internal file stream."""
        if not self._opened:
            self._zip_fp = self.zip_extracted_file.zip_fileobj.open()
            self._zipfile = zipfile.ZipFile(self._zip_fp)
            self._stream = self._zipfile.open(self.zip_extracted_file.zip_path, "r")
            self._opened = True

    def read(self, size=-1):
        """Read data from the stream.

        :param size: Number of bytes to read, or -1 for all
        :returns: Bytes read from the stream
        """
        self._ensure_open()
        return self._stream.read(size)

    def readline(self, size=-1):
        """Read a line from the stream."""
        self._ensure_open()
        return self._stream.readline(size)

    def readlines(self, hint=-1):
        """Read all lines from the stream."""
        self._ensure_open()
        return self._stream.readlines(hint)

    def seek(self, offset, whence=0):
        """Seek to a position in the stream.

        Note: ZipExtFile (returned by ZipFile.open()) has limited seek support.
        It only supports seeking forward from the current position.
        """
        self._ensure_open()
        return self._stream.seek(offset, whence)

    def tell(self):
        """Get current position in the stream."""
        self._ensure_open()
        return self._stream.tell()

    def close(self):
        """Close the stream and all associated resources."""
        if self._opened:
            if self._stream:
                self._stream.close()
            if self._zipfile:
                self._zipfile.close()
            if self._zip_fp:
                self._zip_fp.close()
            self._opened = False

    def __enter__(self):
        """Context manager entry."""
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

    def __iter__(self):
        """Iterate over lines in the file."""
        self._ensure_open()
        return iter(self._stream)

    def __del__(self):
        """Destructor to ensure resources are freed."""
        self.close()
