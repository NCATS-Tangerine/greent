import argparse

def process_file_for_qc(filename, file_path, output_path, impute2_cutoff=0.5,  alt_af_min=0.01, alt_af_max=0.99):
    gwas_file_path = file_path + filename
    try:
        if gwas_file_path.endswith('.gz'):
            isGzip = True
            f = gzip.open(gwas_file_path, mode='rt')
        else:
            isGzip = False
            f = open(gwas_file_path)

    except IOError:
        print('OBH_QC could not open file:' + gwas_file_path)
        return

    try:
        output_file = open(output_path + filename, 'x')
    except FileExistsError:
        print('OBH_QC stopped - that output file already exists')
        f.close()
        return

    with f, output_file:
        headers = next(f).split()
        new_headers = headers[:5]
        new_headers.append('PVALUE')
        try:
            impute2_index = headers.index('IMPUTE2_INFO')
            alt_af_index = headers.index('ALT_AF')
            pvalue_index = headers.index('PVALUE')
        except ValueError:
            print('OBH_QC error reading file headers for' + gwas_file_path)
            return 

        output_file.write('\t'.join(new_headers) + '\n')

        line_counter = 1
        for line in f:
            try:
                line_counter += 1
                data = line.split()
                impute2_score = float(data[impute2_index])
                if impute2_score >= impute2_cutoff:
                    alt_af_freq = float(data[alt_af_index])
                    if alt_af_min <= alt_af_freq <= alt_af_max:
                        clean_data = data[:5]
                        clean_data.append(data[pvalue_index])

                        output_file.write('\t'.join(clean_data) + '\n')

            except (IndexError, ValueError) as e:
                print('OBH_QC error on line' + line_counter + ':' + e)

if __name__=='__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("filename")
    parser.add_argument("data_directory")
    parser.add_argument("output_directory")
    args = parser.parse_args()

    process_file_for_qc(args.filename, args.data_directory, args.output_directory)
