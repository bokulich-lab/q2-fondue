// use bio::io::fastq;
// use itertools::Itertools;
use flate2::write::GzEncoder;
use flate2::Compression;
use pyo3::prelude::{pyfunction, pymodule, PyModule, PyResult, Python};
use pyo3::wrap_pyfunction;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};

#[pymodule]
fn fastq_writer(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(rewrite_fastq, m)?)?;
    Ok(())
}

// #[pyfunction]
// fn rewrite_fastq(fin: &str, fout: &str) -> PyResult<()> {
//     let reader = fastq::Reader::from_file(fin).unwrap();
//     let mut writer = fastq::Writer::to_file(fout).unwrap();
//     for result in reader.records() {
//         let record = &result.unwrap();
//         writer.write_record(&record).unwrap();
//     };
//     Ok(())
// }

// #[pyfunction]
// fn rewrite_fastq(fin: &str, fout: &str) -> PyResult<()> {
//     let file_in = File::open(fin)?;
//     let buff_in = BufReader::new(file_in);
//
//     let file_out = File::create(fout)?;
//     let mut buff_out = BufWriter::new(file_out);
//
//     const N: usize = 4;
//
//     for lines in &buff_in.lines().chunks(N) {
//         for (_i, line) in lines.enumerate() {
//             let l = line.expect("Unable to read line.");
//             buff_out.write(l.as_bytes()).expect("Unable to write to file.");
//             buff_out.write("\n".as_bytes()).expect("Unable to write to file.");
//         }
//     }
//
//     Ok(())
// }

#[pyfunction]
fn rewrite_fastq(fin: &str, fout: &str) -> PyResult<()> {
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

    Ok(())
}

#[cfg(not(test))]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::env::temp_dir;
    use std::io::{BufRead, BufReader};

    fn create_tmp_file(name: &str) -> String {
        let mut dir = temp_dir();
        dir.push(name);
        let _fexp = File::create(&dir).expect("Could not create file");
        let a = format!("{}", &dir.as_path().display());
        a
    }

    fn assert_file_content(f1: &str, f2: &str) {
        let buff1 = BufReader::new(File::open(f1).expect("Could not open file for reading."));
        let buff2 = BufReader::new(GzDecoder::new(File::open(f2).expect("Could not open gz file.")));

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
        let fin = "../data/test_input.fastq";
        let fout = create_tmp_file("test_seq.fastq.gz");
        let fout = fout.as_str();

        println!("Hello!");

        rewrite_fastq(fin, fout).expect("Could not rewrite fastq file");

        assert_file_content(fin, fout);
    }
}
