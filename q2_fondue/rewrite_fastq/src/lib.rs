use bio::io::fastq;
use pyo3::prelude::{pymodule, pyfunction, PyModule, PyResult, Python};
use pyo3::wrap_pyfunction;

#[pymodule]
fn fastq_writer(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rewrite_fastq, m)?)?;
    Ok(())
}

#[pyfunction]
fn rewrite_fastq(fin: &str, fout: &str) -> PyResult<()> {
    let reader = fastq::Reader::from_file(fin).unwrap();
    let mut writer = fastq::Writer::to_file(fout).unwrap();
    for result in reader.records() {
        let record = &result.unwrap();
        writer.write_record(&record).unwrap();
    };
    Ok(())
}
