from ms.mississippi import *
from my_credentials import *                                            


parameters = """www.adobe.com/in/
labs.adobe.com/
www.adobe.com/go/flashplayer_security_sk
blogs.adobe.com/labs/
www.adobe.com/go/fms_dynamicstreaming
www.adobe.com/products/soundbooth/
www.adobe.com/go/buysoundbooth
blogs.adobe.com/digitalpublishing/
https://blogs.adobe.com/digitalpublishinggallery/
www.adobe.com/devnet.html
blogs.adobe.com/jkost/
edex.adobe.com/
tv.adobe.com/
blogs.adobe.com/
feeds.adobe.com/
techlive.adobe.com/
helpx.adobe.com/photoshop.html
helpx.adobe.com/illustrator.html
www.adobe.com/products/photoshopfamily.html
www.adobe.com/products/photoshop.html
www.adobe.com/uk/security.html
www.adobe.com/products/premiere.html
www.adobe.com/products/aftereffects.html
www.adobe.com/products/acrobatstandard.html
www.adobe.com/products/acrobatpro.html
www.adobe.com/news-room.html
www.adobe.com/products/dreamweaver.html
www.adobe.com/downloads/updates.html
www.adobe.com/products/framemakerpublishingserver.html
www.adobe.com/lu_en/cart.html
www.adobe.com/devnet/devices.html
www.adobe.com/au/accessibility.html
www.adobe.com/uk/company.html
www.adobe.com/eeurope/accessibility.html
forums.adobe.com/community/acrobatdotcom
www.adobe.com/products/kuler.html
helpx.adobe.com/dreamweaver.html
forums.adobe.com/community/premiere/
www.adobe.com/products/indesign.html
helpx.adobe.com/lightroom.html
get.adobe.com/uk/reader/
forums.adobe.com/community/photoshop
forums.adobe.com/community/muse
helpx.adobe.com/flash.html
helpx.adobe.com/bridge.html
www.adobe.com/open-source.html
www.adobe.com/products/technicalcommunicationsuite.html
www.adobe.com/solutions/primetime.html
www.adobe.com/solutions/broadcasting.html
forums.adobe.com/community/audition
www.adobe.com/products/contribute.html
www.adobe.com/devnet/security.html
www.adobe.com/uk/accessibility.html
www.adobe.com/devnet/coldfusion.html
www.adobe.com/products/shockwaveplayer.html
www.adobe.com/products/illustrator.html
www.adobe.com/products/fireworks.html
forums.adobe.com/community/business_catalyst
www.adobe.com/devnet/fireworks.html
www.adobe.com/products/robohelp.html
www.adobe.com/products/scene7.html
www.adobe.com/devnet/air.html
gaming.adobe.com/events/gamejams/
blogs.adobe.com/indesigndocs/
help-forums.adobe.com/
tv.adobe.com/de/
www.adobe.com/au/products/cs6.html
helpx.adobe.com/premiere-elements.html
blogs.adobe.com/captivate/2008/10
blogs.adobe.com/educationleaders/2010/03
nvaug.groups.adobe.com/
blogs.adobe.com/jnack/
www.adobe.com/support/security/
www.adobe.com/products/photoshop-lightroom.html
https://blogs.adobe.com/dmspost/
blogs.adobe.com/connectsupport/
forums.adobe.com/community/digital_marketing_suite/cq5
blogs.adobe.com/digitalpublishing/2011/01
marple.host.adobe.com/
forums.adobe.com/community/flashplayer/using_flashplayer
www.adobe.com/products/presenter/elearning.html
www.adobe.com/in/products/acrobat.html
www.adobe.com/uk/accessibility/create.html
edex.adobe.com/discussions/
blogs.adobe.com/adobelife/
tv.adobe.com/sitemap/
tv.adobe.com/shows/
blogs.adobe.com/echosign/tag/salesforce/
www.adobe.com/uk/careers/college.html
chennaicfug.groups.adobe.com/
techlive.adobe.com/speakers/
blogs.adobe.com/educationleaders/tag/createnow
captivate.adobe.com/captivate/products/captivate_adobe_captivate
https://blogs.adobe.com/adobeingovernment/author/blovett
www.adobe.com/be_en/products/testandtarget.html
https://blogs.adobe.com/digitalpublishing/2013/08
helpx.adobe.com/edge-animate.html
blogs.adobe.com/educationleaders/category/tutorials
forums.adobe.com/community/.../installation_and_update_installation
bangalorecfug.groups.adobe.com/
"""


def process(parameters):
    tmp = "/tmp/"
    filename = parameters.replace('/','_').replace('.','-')
    subprocess.call(["wget", "-O", tmp + filename, parameters])
    mkdir("s3n://dl-stl-ml-awsdev/download/")    
    cp("file://" + tmp + filename, "s3n://dl-stl-ml-awsdev/download/" + filename)
    rmr("file://" + tmp + filename)


cluster = EMRCluster(my_access_key_id, my_secret_access_key, my_key_pair_name)
cluster.run_batch_job(process, parameters)
cluster.print_info()