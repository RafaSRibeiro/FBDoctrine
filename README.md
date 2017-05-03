fbdoctrine
==========

A Symfony project created on May 3, 2017, 6:56 pm.

config.yml
# Doctrine Configuration
doctrine:
    dbal:
        driver: ~
        driver_class: RafaSRibeiro\FBDoctrineBundle\DBAL\Driver\Firebird\Driver


AppKernel.php
    public function registerBundles() {
        $bundles = [
            new RafaSRibeiro\FBDoctrineBundle\FBDoctrineBundle(),
        ];