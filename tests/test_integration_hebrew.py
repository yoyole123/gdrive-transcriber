import os
import shutil
import asyncio
from types import SimpleNamespace
from datetime import datetime, timezone

import transcriber.runner as runner_mod
import transcriber.drive as drive_mod
import transcriber.audio as audio_mod
import transcriber.model as model_mod


def test_integration_hebrew_sample(tmp_path, monkeypatch):
    """Integration-style test: simulate a single Drive file (tests/test_assets/hebrew_sample.m4a)

    This test exercises the runner pipeline end-to-end while mocking external
    dependencies: Drive download (copies the local test asset), MP3 conversion
    (copy only), and the transcription model (dummy async generator).

    The test verifies the pipeline runs and produces a processed summary for
    the asset.
    """
    repo_root = os.path.dirname(os.path.dirname(__file__))
    asset_path = os.path.join(repo_root, 'tests', 'test_assets', 'hebrew_sample.m4a')
    assert os.path.exists(asset_path), "Hebrew sample asset not found"

    # Simulate a Drive listing containing a single file
    def fake_list(service, drive_folder_id, skip_drive):
        return [{
            'id': 'fid-hebrew',
            'name': os.path.basename(asset_path),
            'createdTime': datetime.now(timezone.utc).isoformat(),
        }]

    # Simulate download: copy local asset into the destination path
    def fake_download(service, file_id, dst_path, skip_drive):
        shutil.copyfile(asset_path, dst_path)

    # No-op processed folder creation / move
    def fake_get_or_create(service, parent, skip_drive):
        return 'processed-folder'

    def fake_move(service, file_id, new_parent, old_parent, skip_drive):
        return None

    # Replace convert_to_mp3 with a simple copy (avoid calling external ffmpeg)
    def fake_convert(inp, outp):
        shutil.copyfile(inp, outp)

    # Avoid calling ffprobe by making _probe_duration return a small value
    def fake_probe(path):
        return 1.0

    # Dummy model that yields a single text segment containing the filename
    class DummyModel:
        def transcribe_async(self, path, diarize=True):
            async def gen():
                yield type('Seg', (), {'text': f"transcribed:{os.path.basename(path)}"})()
            return gen()

    def fake_load_model(api_key, endpoint_id, languages_cfg, language='he'):
        return DummyModel()

    # Apply monkeypatches
    # Patch the names on the runner module (these were imported there), and
    # patch drive_service to avoid real Google API calls.
    monkeypatch.setattr(runner_mod, 'list_audio_files', fake_list)
    monkeypatch.setattr(runner_mod, 'download_file', fake_download)
    monkeypatch.setattr(runner_mod, 'get_or_create_processed_folder', fake_get_or_create)
    monkeypatch.setattr(runner_mod, 'move_file_to_folder', fake_move)
    monkeypatch.setattr(runner_mod, 'convert_to_mp3', fake_convert)
    monkeypatch.setattr(runner_mod, 'drive_service', lambda skip_drive, sa: None)
    # transcribe_file calls model._probe_duration internally; patch on model_mod
    monkeypatch.setattr(model_mod, '_probe_duration', fake_probe)
    # runner calls load_model (imported into runner); patch runner_mod.load_model
    monkeypatch.setattr(runner_mod, 'load_model', fake_load_model)

    # Build a simple cfg object with required attributes
    cfg = SimpleNamespace()
    cfg.within_schedule_window = True
    cfg.skip_drive = False
    cfg.drive_folder_id = 'dummy'
    cfg.service_account_file = None
    cfg.languages = {'he': {'model': 'dummy'}}
    cfg.runpod_api_key = 'dummy'
    cfg.runpod_endpoint_id = 'dummy'
    cfg.seg_seconds = 30
    cfg.max_segment_concurrency = 1
    cfg.bypass_split = False
    cfg.max_segment_retries = 0
    cfg.max_payload_size = 9 * 1024 * 1024
    cfg.max_split_depth = 3
    cfg.add_random_personal_message = False
    cfg.gmail_app_password = None
    cfg.gmail_sender_email = None
    cfg.email_to = None
    cfg.balance_alert_value = 2.0
    cfg.max_segment_size = 8 * 1024 * 1024

    # Run pipeline
    result = asyncio.run(runner_mod.process_drive_files(cfg))

    assert isinstance(result, dict)
    assert result.get('total_files', 0) == 1
    processed = result.get('processed', [])
    assert len(processed) == 1
    item = processed[0]
    assert item['name'] == os.path.basename(asset_path)
    # The pipeline should have generated at least one segment
    assert item['segments'] >= 1
