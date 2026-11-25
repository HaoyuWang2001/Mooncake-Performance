"""Selective YLT port: request windows, matching report, and raw retention."""
import csv
import json
import sys
from pathlib import Path
from types import SimpleNamespace
import pytest
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
import benchmark as bm


def cfg(**extra):
    value = {'scenario':'kvtest', 'protocol':'ub', 'work_mode':'rank',
        'duration':360, 'report':{'start_s':120,'end_s':330}, 'data_size':['8MB'],
        'base_qps':105,'qps_multiple':1,'batch_size':1,'notify_count':1,
        'client':{'count_per_node':1,'process_mode':'split'},
        'writer':{'operation':'upsert_from','num_threads':1,'notify_threads':1},
        'reader':{'operation':'get_buffer','num_threads':1},
        '_kvtest_start_epochs':{'8MB':1000},'_kvtest_iid_nodes':{'0':'nodeA'}}
    value.update(extra)
    return value


def artifacts(folder):
    folder.mkdir(parents=True)
    base = bm._kvtest_artifact_base('demo',0,9301,'8MB')
    path=folder/(base+'.samples.csv')
    with path.open('w',newline='') as stream:
        writer=csv.writer(stream)
        writer.writerow(['elapsed_us','instance_id','operation','request_index','latency_ms','status','bytes'])
        for op in ('upsert_from','get_buffer','notify_queue_wait','notify_rpc'):
            for second,latency in ((119,999),(120,2),(329,4),(330,999)):
                writer.writerow([second*1000000,0,op,second,latency,'OK',1048576 if op in ('upsert_from','get_buffer') else 0])
    summary=folder/(base+'.summary.json')
    summary.write_text(json.dumps({'elapsed_seconds':360,'verify_failures':99,'operations':{}}))
    return path,summary


def test_csv_report_and_raw_preservation(tmp_path):
    logs=tmp_path/'logs'/'demo'
    samples,summary=artifacts(logs)
    originals={p:p.read_bytes() for p in (samples,summary)}
    config=cfg()
    rows=bm.write_scenario_csv(str(logs),config,'run',str(tmp_path/'demo.json'),result_dir=str(tmp_path))
    assert len(rows)==4
    for row in rows:
        assert row['success']==2
        assert row['wall_s']==210
        assert row['avg_ms']==3
        assert row['max_ms']==4
        assert row['total_bytes']==2097152
        assert row['throughput_mb_s']==round(2/210,2)
    reader=next(r for r in rows if r['role']=='reader')
    assert reader['queue_wait_avg_ms']==3
    report=bm._kvtest_generate_window_report(str(logs),config,rows)
    assert Path(report).exists()
    data=json.loads((logs/'report'/'stats.json').read_text(encoding='utf-8'))
    assert data['rows']==rows
    assert data['report_window']==[120,330]
    meta=logs/bm.KVTEST_REPORT_META
    assert json.loads(meta.read_text(encoding='utf-8'))['report']==config['report']
    bm._cleanup_kvtest_success_intermediates(str(tmp_path/'logs'))
    assert meta.exists()
    for p,original in originals.items():
        assert p.read_bytes()==original


@pytest.mark.parametrize('start,end',[(-1,330),(120,120),(120,361),(float('nan'),330)])
def test_invalid_window(start,end):
    with pytest.raises(ValueError):
        bm._kvtest_report_window(cfg(report={'start_s':start,'end_s':end}))


def test_early_stop(tmp_path):
    samples,summary=artifacts(tmp_path/'logs')
    summary.write_text('{"elapsed_seconds":180}')
    doc=bm._kvtest_window_summary(str(summary),cfg())
    assert doc['elapsed_seconds']==60
    assert doc["operations"]["get_buffer"]["success"] == 1


def test_missing_samples_rejected(tmp_path):
    summary=tmp_path/'case.summary.json'
    summary.write_text('{"elapsed_seconds":360}')
    with pytest.raises(FileNotFoundError):
        bm._kvtest_window_summary(str(summary),cfg())


def test_launcher_forwards_window():
    from scripts.run_rank_recall import _child_argv
    args=SimpleNamespace(rank_cluster='c',rank_global='g',profile='p',dry_run=False,
        no_warmup=False,no_plot=True,verbose=False,protocol=None,stop_file=None,
        report_start_s=120,report_end_s=330)
    argv=_child_argv(args,'rank',['r.json'],'parent','ready')
    assert argv[argv.index('--report-start-s')+1]=='120'
    assert argv[argv.index('--report-end-s')+1]=='330'
