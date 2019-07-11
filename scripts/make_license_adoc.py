import yaml

header="""== Data Sources, Licenses, and Attributions
++++
.Sources
|===
|Data Source |License| Attribution

"""

mydata = yaml.load(open("../source_licenses.yaml"))
#print(mydata)

url='url'
name='name'
license='license'
license_url='license_url'
citation_url='citation_url'

with open('../deploy/robokopkg/webserver/app/adocs/licenses.adoc','w') as outf:
    outf.write(header)
    for source in mydata:
        print(source[name])
        outf.write(f'|<a href="{source[url]}">{source[name]}</a> \n')
        outf.write(f'|<a href="{source[license_url]}">{source[license]}</a> \n')
        if citation_url in source:
            outf.write(f'|<a href="{source[citation_url]}">Citation</a> \n')
        else:
            outf.write('|\n')   
        outf.write('\n')
    outf.write('|===\n')
    outf.write('++++\n')
