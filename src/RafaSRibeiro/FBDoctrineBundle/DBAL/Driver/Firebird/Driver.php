<?php

/*
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * This software consists of voluntary contributions made by many individuals
 * and is licensed under the MIT license. For more information, see
 * <http://www.doctrine-project.org>.
 */

namespace RafaSRibeiro\FBDoctrineBundle\DBAL\Driver\Firebird;

use Doctrine\DBAL\DBALException;
use RafaSRibeiro\FBDoctrineBundle\DBAL\Driver\AbstractFirebirdDriver;
use Doctrine\DBAL\Driver\PDOException;

/**
 * The PDO Firebird driver.
 *
 */
class Driver extends AbstractFirebirdDriver {

    /**
     * @var array
     */
    protected $_userDefinedFunctions = array(
        'sqrt' => array('callback' => array('FBDoctrine\DBAL\Platforms\FirebirdPlatform', 'udfSqrt'), 'numArgs' => 1),
        'mod' => array('callback' => array('FBDoctrine\DBAL\Platforms\FirebirdPlatform', 'udfMod'), 'numArgs' => 2),
        'locate' => array('callback' => array('FBDoctrine\DBAL\Platforms\FirebirdPlatform', 'udfLocate'), 'numArgs' => -1),
    );

    /**
     * {@inheritdoc}
     */
    public function connect(array $params, $username = null, $password = null, array $driverOptions = array()) {

        if (isset($driverOptions['userDefinedFunctions'])) {
            $this->_userDefinedFunctions = array_merge(
                    $this->_userDefinedFunctions, $driverOptions['userDefinedFunctions']);
            unset($driverOptions['userDefinedFunctions']);
        }

        try {
            $pdo = new FirebirdConnection(
                    $this->_constructPdoDsn($params), 
                    $params['user'], 
                    $params['password'], 
                    $driverOptions
            );
        } catch (PDOException $ex) {
            throw DBALException::driverException($this, $ex);
        }

//        foreach ($this->_userDefinedFunctions as $fn => $data) {
//            $pdo->sqliteCreateFunction($fn, $data['callback'], $data['numArgs']);
//        }

        return $pdo;
    }

    /**
     * Constructs the Sqlite PDO DSN.
     *
     * @param array $params
     *
     * @return string The DSN.
     */
    protected function _constructPdoDsn(array $params) {

        $port = null;
        if (!$params['port']) {
            $port = '/3050';
        }

        $dsn = 'firebird:host=' . $params['host'] . ';dbname=' . $params['dbname'];// . ';charset=' . $params['charset'] . ';';
        
        return $dsn;
    }

    /**
     * {@inheritdoc}
     */
    public function getName() {
        return 'pdo_firebird';
    }

}
