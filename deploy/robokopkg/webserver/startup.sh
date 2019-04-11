
adoc_path=$HOME/robokopkg/adocs/
templates_path=$HOME/robokopkg/templates

for adoc in $(ls $adoc_path)
do
    #get the file name with out the extension
    file_name=(${adoc//./ })
    $HOME/neo4j-guides/run.sh $adoc_path/$adoc $templates_path/guide/$file_name.html  
    echo "Created Jinja template from $adoc"  
done

# flask run -h 0.0.0.0 -p 5000 
gunicorn --bind 0.0.0.0:5000 wsgi:app
