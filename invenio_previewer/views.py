# -*- coding: utf-8 -*-
#
# This file is part of Invenio.
# Copyright (C) 2016-2019 CERN.
#
# Invenio is free software; you can redistribute it and/or modify it
# under the terms of the MIT License; see LICENSE file for more details.

"""View method for Invenio-Records-UI for previewing files."""

from flask import Blueprint, abort, current_app, request

from .api import create_preview_file
from .extensions import default
from .proxies import current_previewer

blueprint = Blueprint(
    "invenio_previewer",
    __name__,
    template_folder="templates",
    static_folder="static",
)
"""Blueprint used to register template and static folders."""


def preview(pid, record, template=None, **kwargs):
    """Preview file for given record.

    Plug this method into your ``RECORDS_UI_ENDPOINTS`` configuration:

    .. code-block:: python

        RECORDS_UI_ENDPOINTS = dict(
            recid=dict(
                # ...
                route='/records/<pid_value/preview/<path:filename>',
                view_imp='invenio_previewer.views.preview',
                record_class='invenio_records_files.api:Record',
            )
        )

    Supports previewing files within ZIP archives via 'zip_path' query parameter:

        /records/123/preview/archive.zip?zip_path=folder/image.png

    The file is streamed directly from the ZIP without full extraction,
    allowing preview of large files (images, PDFs, etc.) without memory overhead.
    """
    # Get file from record
    filename = request.view_args.get("filename", request.args.get("filename", type=str))
    fileobj = current_previewer.record_file_factory(pid, record, filename)

    if not fileobj:
        abort(404)

    # Create the appropriate file object (regular or ZIP-extracted)
    zip_path = request.args.get("zip_path")
    try:
        fileobj = create_preview_file(pid, record, fileobj, zip_path)
    except ValueError as e:
        # Invalid container type (e.g., zip_path on non-ZIP file)
        abort(400, str(e))
    except FileNotFoundError as e:
        # File not found in ZIP archive
        abort(404, str(e))
    except Exception as e:
        current_app.logger.error(
            f"Error creating preview file: {str(e)}", exc_info=True
        )
        abort(500, "Error accessing file")

    # Try to see if specific previewer is set (only for non-ZIP files)
    file_previewer = None
    if not zip_path:
        file_previewer = (
            fileobj.file.get("previewer")
            if hasattr(fileobj, "file") and isinstance(fileobj.file, dict)
            else None
        )
    print(f"{list(current_previewer.iter_previewers(None))=}")
    # Find a suitable previewer and render
    for plugin in current_previewer.iter_previewers(
        previewers=[file_previewer] if file_previewer else None
    ):
        if plugin.can_preview(fileobj):
            try:
                return plugin.preview(fileobj)
            except Exception:
                current_app.logger.warning(
                    (
                        "Preview failed for {key}, in {pid_type}:{pid_value}".format(
                            key=fileobj.file.key,
                            pid_type=fileobj.pid.pid_type,
                            pid_value=fileobj.pid.pid_value,
                        )
                    ),
                    exc_info=True,
                )
    return default.preview(fileobj)


def file_download_ui(pid, record, template=None, **kwargs):
    """Download file from record, with support for ZIP extraction.

    This wraps the standard file download functionality and adds support
    for extracting and streaming files from within ZIP archives.

    Use this in your RECORDS_UI_ENDPOINTS configuration:

    .. code-block:: python

        RECORDS_UI_ENDPOINTS = dict(
            recid_files=dict(
                pid_type='recid',
                route='/record/<pid_value>/files/<filename>',
                view_imp='invenio_previewer.views:file_download_ui',
                record_class='invenio_records_files.api:Record',
            )
        )

    Supports extracting files from ZIP archives via 'zip_path' query parameter:

        /record/123/files/archive.zip?zip_path=folder/image.png

    The file is streamed directly from the ZIP without full extraction.
    """
    import mimetypes

    from flask import Response

    # Get file from record
    filename = request.view_args.get("filename")
    fileobj = current_previewer.record_file_factory(pid, record, filename)

    if not fileobj:
        abort(404)

    # Check if we're downloading a file from within a ZIP archive
    zip_path = request.args.get("zip_path")

    if zip_path:
        # Use factory to create ZipExtractedFile
        try:
            extracted_file = create_preview_file(pid, record, fileobj, zip_path)
        except ValueError as e:
            abort(400, str(e))
        except FileNotFoundError as e:
            abort(404, str(e))
        except Exception as e:
            current_app.logger.error(
                f"Error accessing file in ZIP: {str(e)}", exc_info=True
            )
            abort(500, "Error accessing file in ZIP archive")

        # Determine MIME type from filename
        mimetype, _ = mimetypes.guess_type(extracted_file.filename)
        if mimetype is None:
            mimetype = "application/octet-stream"

        # Stream the file from the ZIP
        file_stream = extracted_file.open()

        # Return the file as a streaming response
        return Response(
            file_stream,
            mimetype=mimetype,
            headers={
                "Content-Disposition": f'inline; filename="{extracted_file.filename}"',
                "Content-Length": str(extracted_file.size),
            },
        )
    else:
        # Standard file download - delegate to the original function
        from invenio_records_files.utils import file_download_ui as original_download

        return original_download(pid, record, **kwargs)


@blueprint.app_template_test("previewable")
def is_previewable(extension):
    """Test if a file can be previewed checking its extension."""
    return extension in current_previewer.previewable_extensions
