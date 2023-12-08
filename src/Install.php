<?php

namespace Sai97\WebManAmqp;

class Install
{
    const WEBMAN_PLUGIN = true;

    /**
     * @var array
     */
    protected static array $pathRelation = [
        'config/plugin/sai97/webman-amqp' => 'config/plugin/sai97/webman-amqp'
    ];


    /**
     * Install
     * @return void
     */
    public static function install(): void
    {
        static::installByRelation();
    }

    /**
     * Uninstall
     * @return void
     */
    public static function uninstall(): void
    {
        self::uninstallByRelation();
    }

    /**
     * installByRelation
     * @return void
     */
    public static function installByRelation(): void
    {
        foreach (static::$pathRelation as $source => $dest) {
            if ($pos = strrpos($dest, '/')) {
                $parent_dir = base_path() . '/' . substr($dest, 0, $pos);
                if (!is_dir($parent_dir)) {
                    mkdir($parent_dir, 0777, true);
                }
            }
            copy_dir(__DIR__ . "/$source", base_path() . "/$dest");
        }
    }

    /**
     * uninstallByRelation
     * @return void
     */
    public static function uninstallByRelation(): void
    {
        foreach (static::$pathRelation as $dest) {
            $path = base_path() . "/$dest";
            if (!is_dir($path) && !is_file($path)) {
                continue;
            }
            if (is_file($path) || is_link($path)) {
                unlink($path);
                continue;
            }
            remove_dir($path);
        }
    }
}