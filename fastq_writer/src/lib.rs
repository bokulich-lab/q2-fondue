use flate2::write::GzEncoder;
use flate2::Compression;
use pyo3::prelude::{pymodule, PyModule, PyResult, Python};
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

#[pymodule]
fn fastq_writer(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    fn rewrite_fastq(fin: &str, fout: &str) {
        _rewrite(fin, fout)
    }

    #[pyfn(m, "rewrite_fastq")]
    fn rewrite_fastq_py<'py>(_py: Python<'py>, fin: &str, fout: &str) {
        rewrite_fastq(fin, fout)
    }

    Ok(())
}

fn _rewrite(fin: &str, fout: &str) {
    let buff_in = BufReader::new(File::open(fin).expect("Could not open file for reading."));
    let mut buff_out = GzEncoder::new(
        File::create(fout).expect("Could not open file for writing."),
        Compression::default(),
    );

    for line in buff_in.lines() {
        let l = line.expect("Unable to read line.");
        buff_out
            .write(l.as_bytes())
            .expect("Unable to write sequence to file.");
        buff_out
            .write("\n".as_bytes())
            .expect("Unable to write to file.");
    }
}

mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::env::temp_dir;
    use std::io::{BufRead, BufReader};

    #[allow(dead_code)]
    fn create_tmp_file(name: &str) -> String {
        let mut dir = temp_dir();
        dir.push(name);
        let _fexp = File::create(&dir).expect("Could not create file");
        let a = format!("{}", &dir.as_path().display());
        a
    }

    #[allow(dead_code)]
    fn assert_file_content(f1: &str, f2: &str) {
        let buff1 = BufReader::new(File::open(f1).expect("Could not open file for reading."));
        let buff2 = BufReader::new(GzDecoder::new(
            File::open(f2).expect("Could not open gz file."),
        ));

        let it = buff1.lines().zip(buff2.lines());
        for (l1, l2) in it {
            assert_eq!(
                l1.expect("Unable to read line from file 1."),
                l2.expect("Unable to read line from file 2.")
            )
        }
    }

    #[test]
    fn test_rewrite_ok() {
        let fin = "./data/test_input.fastq";
        let _fout = create_tmp_file("test_seq.fastq.gz");
        let fout = _fout.as_str();

        _rewrite(fin, fout);

        assert_file_content(fin, fout);
    }
}
